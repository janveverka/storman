package T0::Copy::Worker;
use strict;
use warnings;

use POE qw( Filter::Reference Component::Client::TCP );
use Sys::Hostname;
use File::Basename;
use T0::Util;
use T0::Castor;
use LWP::Simple;
use Socket;
use JSON qw( to_json );

use Carp;
our $VERSION = 1.00;
our @ISA     = qw/ Exporter /;

our $hdr = __PACKAGE__ . ':: ';
sub Croak { croak $hdr, @_; }
sub Carp  { carp $hdr,  @_; }
sub Verbose { T0::Util::Verbose( (shift)->{Verbose}, @_ ); }
sub Debug { T0::Util::Debug( (shift)->{Debug}, @_ ); }
sub Quiet { T0::Util::Quiet( (shift)->{Quiet}, @_ ); }

# Format a runnumber into a directory
# Eg: 1234567 => 001/234/567
sub format_run {
    my $runnumber = shift;
    return '' unless $runnumber;
    my $format = sprintf( "%09d", $runnumber );
    for ($format) {
        s|(\d\d\d)|$1/|g;
        s|/$||;
    }
    return $format;
}

sub new {
    my $class = shift;
    my $self  = {};
    bless( $self, $class );

    my %h = @_;
    @$self{ keys %h } = values %h;

    $self->ReadConfig();

    $self->{Host} = hostname();
    $self->{Name} .= '-' . $self->{Host} . '-' . $$;

    POE::Component::Client::TCP->new(
        RemotePort     => $self->{Manager}->{Port},
        RemoteAddress  => $self->{Manager}->{Host},
        Alias          => $self->{Name},
        Filter         => "POE::Filter::Reference",
        ConnectTimeout => 5,
        ServerError    => \&server_error,
        ConnectError   => \&_connection_error_handler,
        Disconnected   => \&_connection_error_handler,
        Connected      => \&_connected,
        PreConnect     => \&_pre_connect,
        ServerInput    => \&_server_input,
        Started        => \&start_task,
        Args           => [$self],
        ObjectStates   => [
            $self => {
                server_input             => 'server_input',
                connected                => 'connected',
                connection_error_handler => 'connection_error_handler',
                copy_done                => 'copy_done',
                job_done                 => 'job_done',
                get_work                 => 'get_work',
                get_client_work          => 'get_client_work',
                should_get_work          => 'should_get_work',
                get_client_id            => 'get_client_id',
                get_beam_status          => 'get_beam_status',
                get_bandwidth            => 'get_bandwidth',
                update_client_id         => 'update_client_id',
                update_beam_status       => 'update_beam_status',
                update_bandwidth         => 'update_bandwidth',
                sig_abort                => 'sig_abort',
                terminate                => 'terminate',
                _stop                    => 'terminate',
                send                     => 'send',
                _default                 => 'handle_default',
            },
        ],
    );

    return $self;
}

# Cleanly shutdown
sub terminate {
    my ( $self, $kernel, $heap ) = @_[ OBJECT, KERNEL, HEAP ];

    # Remove timers
    my @removed_alarms = $kernel->alarm_remove_all();
    foreach my $alarm (@removed_alarms) {
        my ( $name, $time, $param ) = @$alarm;
        $self->Verbose("Remove alarm $name\n");
    }
    delete $self->{RetryInterval};    # Do not try to reconnect

    my $queue_size = keys %{ $heap->{HashRef} };
    if ($queue_size) {
        $self->Verbose("Trying to salvage $queue_size events\n");
        for my $id (
            sort {
                $heap->{HashRef}->{$a}->{id} <=> $heap->{HashRef}->{$b}->{id}
            }
            keys %{ $heap->{HashRef} }
          )
        {
            $self->Verbose(
                "Trying to salvage event $heap->{HashRef}->{$id}->{id}\n");

            # XXX should send it back to the server, that is the easiest
        }
        $kernel->delay( 'shutdown' => 10 );
    }
    else {
        $kernel->yield('shutdown');
    }
    if ( $self->{Logger} ) {
        $kernel->post( $self->{Logger}->{Name}, 'forceShutdown' );
    }
    if ( $kernel->alias_resolve($castor_alias) ) {
        $kernel->alias_remove($castor_alias);
    }
}

# Some terminal signal got received
sub sig_abort {
    my ( $self, $kernel, $heap, $signal ) = @_[ OBJECT, KERNEL, HEAP, ARG0 ];

    $self->Verbose("Shutting down on signal SIG$signal.\n");
    $kernel->yield('terminate');
    $kernel->sig_handled();
}

sub start_task {
    my ( $heap, $self, $kernel ) = @_[ HEAP, ARG0, KERNEL ];

    # put some parameters on heap
    $heap->{Node}  = $self->{Node};
    $heap->{State} = 'Start';

    $kernel->yield('update_bandwidth'); # Calculate bandwidth usage every minute
    $kernel->yield('update_beam_status');    # Get beam status every minute

    # $_[SESSION]->option( trace => 1 ); # XXX tracing
    # Try to shutdown nicely, that is letting copies finish
    $kernel->sig( INT  => 'sig_abort' );
    $kernel->sig( TERM => 'sig_abort' );
    $kernel->sig( QUIT => 'sig_abort' );
}

sub ReadConfig {
    my $self = shift;
    my $file = $self->{Config};

    return unless $file;
    $self->Quiet("Reading configuration file $file\n");

    my $n = $self->{Name};
    $n =~ s%Worker.*$%Manager%;
    $self->{Partners} = { Manager => $n };
    $n = $self->{Name};
    $n =~ s%-.*$%%;
    T0::Util::ReadConfig( $self,, $n );

    $self->{Channels}->{$_} = 1 for @{ $T0::System{Channels} };
}

sub server_error { Print $hdr, " Server error\n"; }

sub _connection_error_handler { reroute_event( ( caller(0) )[3], @_ ); }

sub connection_error_handler {
    my ( $self, $kernel ) = @_[ OBJECT, KERNEL ];

    my $retry = $self->{RetryInterval};
    return unless $retry;

    if ( !$self->{Retries}++ ) {
        Print $hdr, " Connection retry every $retry seconds\n";
    }
    $kernel->delay( reconnect => $retry );
}

sub send {
    my ( $self, $kernel, $heap, $session, $hash_ref ) =
      @_[ OBJECT, KERNEL, HEAP, SESSION, ARG0 ];
    $hash_ref = \$hash_ref unless ref($hash_ref);
    if ( $heap->{connected} && $heap->{server} ) {
        $heap->{server}->put($hash_ref);
    }
}

sub prepare_work {
    my ( $self, $work ) = @_;

    # deciding what parameters set to apply according to the destination
    my $dsparams;
    if (   exists( $work->{DESTINATION} )
        && exists( $self->{DestinationConfiguration}->{ $work->{DESTINATION} } )
      )
    {
        $dsparams = $self->{DestinationConfiguration}->{ $work->{DESTINATION} };
    }
    else {
        $dsparams = $self->{DestinationConfiguration}->{default};
    }

    # feed parameters into work hash
    @$work{ keys %$dsparams } = values %$dsparams;

    my ( $day, $month, $year ) = ( localtime(time) )[ 3, 4, 5 ];
    $month += 1;
    $year  += 1900;
    my $rundir     = format_run( $work->{RUNNUMBER} );
    my $stream     = $work->{STREAM} ? "$work->{STREAM}/" : '';
    my $setuplabel = $work->{SETUPLABEL};
    my $lfndir     = '';

    if ( $work->{TargetDir} eq '/dev/null' ) {
        $work->{PFN} = '/dev/null';
    }
    elsif ( defined( $work->{SplitMode} ) ) {
        $work->{InjectIntoTier0} = 0;
        if ( $work->{SplitMode} eq 'tier0StreamerNewPoolsLFN' ) {
            $lfndir = "/store/t0streamer/$setuplabel/$stream$rundir";
            $work->{LFN} = $lfndir . '/' . $work->{FILENAME};
            $work->{InjectIntoTier0} = 1;
        }
        elsif ( $work->{SplitMode} eq 'tier0StreamerLFN' ) {
            $lfndir = "/store/streamer/$setuplabel/$stream$rundir";
            $work->{LFN} = $lfndir . '/' . $work->{FILENAME};
            $work->{InjectIntoTier0} = 1;
        }
        elsif ( $work->{SplitMode} eq 'streamerLFN' ) {
            $lfndir = "/store/streamer/$setuplabel/$stream$rundir";
            $work->{LFN} = $lfndir . '/' . $work->{FILENAME};
        }
        elsif ( $work->{SplitMode} eq 'lumiLFN' ) {
            if ( $work->{FILENAME} =~
/CMS_LUMI_RAW_([0-9][0-9][0-9][0-9])([0-9][0-9])([0-9][0-9])_([0-9]+)_([^.]+).root/
              )
            {
                $lfndir = sprintf( "/store/lumi/%04d%02d", $1, $2 );
            }
            else {
                $lfndir = sprintf( "/store/lumi/%04d%02d", $year, $month );
            }
            $work->{LFN} = $lfndir . '/' . $work->{FILENAME};
        }
        elsif ( $work->{SplitMode} eq 'dqmLFN' ) {
            $lfndir =
              sprintf( "/store/temp/dqm/online/%04d%02d", $year, $month );
            $work->{LFN} = $lfndir . '/' . $work->{FILENAME};
        }
        elsif ( $work->{SplitMode} eq 'daily' ) {
            $lfndir = sprintf( "/%04d%02d%02d", $year, $month, $day );
        }
    }

    #
    # Deprecated, remove all these lines and replace by
    # and else that prints out an error message
    #
    elsif ( $work->{CreateLFN} ) {
        $lfndir = "/store/data/$setuplabel/$stream$rundir";
        $work->{LFN} = $lfndir . "/" . $work->{FILENAME};
    }
    elsif ( $work->{SplitByDay} ) {
        $lfndir = sprintf( "/%04d%02d%02d", $year, $month, $day );
    }
    $work->{TargetDir} .= $lfndir;
    $work->{PFN} = $work->{TargetDir} . '/' . $work->{FILENAME};
}

sub _server_input { reroute_event( ( caller(0) )[3], @_ ); }

sub server_input {
    my ( $self, $kernel, $heap, $session, $hash_ref ) =
      @_[ OBJECT, KERNEL, HEAP, SESSION, ARG0 ];

    my $command = $hash_ref->{command};

    $self->Quiet("from server: $command\n");
    if ( $command =~ m%Sleep% ) {

        # ask again in as many seconds as the server said
        my $wait = $hash_ref->{wait} || 10;
        $self->Quiet("Sleeping $wait seconds, as ordered\n");
        $kernel->delay( should_get_work => $wait );
        return;
    }

    if ( $command =~ m%DoThis% ) {
        $heap->{State} = 'Busy';

        my $work = $hash_ref->{work};

        # nothing went wrong yet
        $hash_ref->{status} = 0;

        # source file
        my $sourcefile = $work->{PATHNAME} . '/' . $work->{FILENAME};

        # configuring target dir, pfn and lfn
        $self->prepare_work($work);

        # target file
        my $targetfile = $work->{PFN};

        # Save things for completion, and remember start time
        $heap->{HashRef}->{ $hash_ref->{id} } = $hash_ref;
        $work->{WorkStarted} = time;

        # set DeleteBadFiles option
        my $deletebadfiles =
          defined( $work->{DeleteBadFiles} )
          ? $work->{DeleteBadFiles}
          : $self->{DeleteBadFiles};

        my %rfcphash = (
            id               => $hash_ref->{id},
            svcclass         => $work->{SvcClass},
            session          => $self->{Name},
            callback         => 'copy_done',
            max_workers      => $self->{MaxWorkers},
            timeout          => $work->{TimeOut},
            retries          => $work->{Retry},
            retry_backoff    => $work->{RetryBackoff},
            delete_bad_files => $deletebadfiles,
            files            => [],
        );

        $self->Debug(
            "Copy $hash_ref->{id} added " . to_json( \%rfcphash ) . "\n" );

        # check source file for existence and file size
        my $filesize = -s $sourcefile;
        if ( !-f _ || ( $work->{FILESIZE} != $filesize ) ) {
            $self->Quiet(
"Source file does not exist or does not match file size in notification\n"
            );
            $hash_ref->{status} = -1;
            $kernel->yield('job_done');
            return;
        }

        $self->Verbose("Copy $hash_ref->{id} queued\n");

        push @{ $rfcphash{files} },
          {
            source   => $sourcefile,
            target   => $targetfile,
            checksum => $work->{CHECKSUM},
            logdir   => format_run( $work->{RUNNUMBER} ),
            size     => $filesize,
          };

        T0::Castor->new( \%rfcphash );
        $kernel->yield('should_get_work');
        return;
    }

    if ( $command =~ m%Setup% ) {
        my $setup = $hash_ref->{setup};
        $self->Verbose( "Setup: " . strhash($setup) . "\n" );
        @$self{ keys %$setup } = values %$setup;
        T0::Castor->new( { max_workers => $self->{MaxWorkers} } );
        $kernel->yield('should_get_work');
        return;
    }

    if ( $command =~ m%Start% ) {
        $kernel->yield('should_get_work');
        return;
    }

    if ( $command =~ m%Stop% ) {
        $heap->{State} = 'Stop';
        $kernel->yield('sig_abort');
        return;
    }

    if ( $command =~ m%Quit% ) {
        $heap->{State} = 'Stop';
        $kernel->yield('should_get_work');
        return;
    }

    if ( $command =~ m%SetID% ) {
        my $clientID = $hash_ref->{clientID};
        $self->Debug("Got $command = $clientID\n");
        $heap->{clientID} = $clientID;
        $kernel->delay( update_client_id => 300 );   # Update ID every 5 minutes
        return;
    }

    Print "Error: unrecognised input from server! \"$command\"\n";
    $kernel->yield('sig_abort');
}

sub _connected { reroute_event( ( caller(0) )[3], @_ ); }

sub connected {
    my ( $self, $heap, $kernel, $hash_ref ) = @_[ OBJECT, HEAP, KERNEL, ARG0 ];
    use Data::Dumper;
    $self->Debug( "handle_connect: from server: " . Dumper($hash_ref) . "\n" );
    $kernel->yield(
        send => {
            'command'  => 'HelloFrom',
            'client'   => $self->{Name},
            'hostname' => $self->{Host},
        }
    );
}

# Make client socket keepalive, so OS will detect dead peers automatically
# see net.ipv4.tcp_keepalive_* for details
sub _pre_connect {
    my $socket = $_[ARG0];

    setsockopt( $socket, SOL_SOCKET, SO_KEEPALIVE, 1 )
      or die "Cannot set socket keepalive: $!";
    return $socket;
}

sub should_get_work {
    my ( $self, $kernel, $session, $heap, $hash_ref ) =
      @_[ OBJECT, KERNEL, SESSION, HEAP, ARG0 ];

    # Check if we're shutting down
    if ( $heap->{State} eq 'Stop' ) {
        $kernel->yield('sig_abort');
        return;
    }

    my $delay        = 60;
    my $minWorkers   = $self->{MinWorkers} || 3;
    my $maxWorkers   = $self->{MaxWorkers} || 99;
    my $extraWorkers = $self->{ExtraWorkers} || 0;
    my $id           = $kernel->call( $session, 'get_client_id' );

    # Check we got a proper ID, otherwise sleep, hoping we'll get a better one
    if ( $id < 1 or $id > 100 ) {
        $self->Verbose(
            "WARNING: Got bad $id as id! Retrying in $delay seconds\n");
        $kernel->delay( should_get_work => $delay );
        return;
    }

    # If our ID is less than the minimum number of workers, request work
    if ( $id <= $minWorkers ) {
        $kernel->yield('get_work');
        return;
    }

    # Ensure we're not running too many workers
    if ( $id > $maxWorkers ) {
        $self->Debug(
            "We are ID $id, and maxWorkers is $maxWorkers. Sleeping 5 min.\n");
        $kernel->delay( should_get_work => 300 );
    }

    # Treat nodes 12 and 13 on rack 7 differently as they have smaller disks
    my $hostname = $self->{Host};
    if (   $hostname =~ /^srv-c2c07-1[23]$/xmsio
        && $id <= $minWorkers + $extraWorkers )
    {
        $self->Debug(
"We are ID $id on $hostname. Would have requested work as $id <= $minWorkers + $extraWorkers\n"
        );

        #    $kernel->yield('get_work');
        #    return;
    }

    # Check run status
    my $beamStatus = $kernel->call( $session, 'get_beam_status' );
    if ( $beamStatus =~ /^(?:SQUEEZE | ADJUST | STABLE | UNKNOWN )$/xms ) {
        $self->Debug(
            "Not interfill, beam is in $beamStatus. Sleeping $delay seconds\n");

        #    $kernel->delay( should_get_work => $delay );
        #    return;
    }

    # Check the bandwidth usage
    my $inData = $kernel->call( $session, 'get_bandwidth', Data => 'rxrate' );
    my $outTier0 =
      $kernel->call( $session, 'get_bandwidth', Tier0 => 'txrate' );
    $self->Debug(
"We are ID $id on $hostname. Incoming bandwidth is $inData. Out: $outTier0\n"
    );
    if ( $inData < 90 ) {
        $self->Debug("Bandwidth is small ($inData), starting $id\n");

        #    $kernel->yield('get_work');
        #    return;
    }
    $kernel->yield('get_client_work');
}

# Return a cached version of the beam status
sub get_beam_status {
    my ( $self, $heap ) = @_[ OBJECT, HEAP ];

    return 'UNKNOWN' if !exists $heap->{beamStatus};
    return $heap->{beamStatus};
}

sub update_beam_status {
    my ( $self, $heap, $kernel ) = @_[ OBJECT, HEAP, KERNEL ];

    my $status = 'UNKNOWN';
    my $level0_url =
'http://srv-c2d04-19.cms:9941/urn:xdaq-application:lid=400/retrieveCollection?fmt=plain&flash=urn:xdaq-flashlist:levelZeroFM_dynamic';
    my $content = get $level0_url;
    my %level0;
    if ( !defined $content ) {
        $self->Quiet(
            "Could not get beam status from Level-0. Probably not running.");
    }
    else {
        my ( $headers, $values ) = split /\n/, $content;
        $values =~ s/\[[^\]]*]//g;
        @level0{ split /,/, $headers } = split /","/, $values;
    }
    my @lhc_modes = qw( MACHINE_MODE BEAM_MODE CLOCK_STABLE );
    $heap->{lhc}->{$_} = $level0{ 'LHC_' . $_ } for @lhc_modes;
    $self->Debug(
        'LHC status: ' . join(
            ', ',
            map {
                    "$_ => '"
                  . ( defined $heap->{lhc}->{$_} ? $heap->{lhc}->{$_} : '' )
                  . "'"
            } @lhc_modes
        ),
        "\n"
    );
    $kernel->delay( update_beam_status => 60 );
}

# Return the bandwidth usage as already calculated per card or network
sub get_bandwidth {
    my ( $self, $heap, $network, $flow ) = @_[ OBJECT, HEAP, ARG0, ARG1 ];

    return 0 if !exists $heap->{'network'}->{$network};
    return $heap->{'network'}->{$network}->{$flow} || 0;
}

# Updates periodically the bandwidth usage
sub update_bandwidth {
    my ( $self, $heap, $kernel ) = @_[ OBJECT, HEAP, KERNEL ];

    my $devfile        = '/proc/net/dev';
    my $routefile      = '/proc/net/route';
    my %networkLookups = (
        A00B => 'Service',
        A03B => 'Data',
        A08B => 'Tier0',
    );

    my @columns;
    open my $netdev, '<', $devfile or die "Can't open $devfile: $!";
  LINE:
    while (<$netdev>) {

   #Inter-|   Receive                                                |  Transmit
        next LINE if /^Inter-/;    # Skip first line

# face |bytes    packets errs drop fifo frame compressed multicast|bytes    packets errs drop fifo colls carrier compressed
        if (/\|/) {
            my @headers = split /\|/;
            shift @headers;        # Discard 'face'
            push @columns, map { "rx$_" } split /\s+/, shift @headers; # Receive
            push @columns, map { "tx$_" } split /\s+/,
              shift @headers;    # Transmit
            next LINE;
        }
        next LINE unless s/^\s*(\w+):\s*//;    # Skip virtual interfaces eth0:1

        # We have a real interface, calculate the bandwidth in and out
        my $card = $1;
        my %hash = ( lastUpdate => time() );
        @hash{@columns} = split /\s+/;
        if ( my $lastValues = delete $heap->{'network'}->{$card} ) {
            if ( my $lastUpdate = $lastValues->{lastUpdate} ) {
                my $timeDiff = $hash{lastUpdate} - $lastUpdate;
                next LINE unless $timeDiff;
                my $unitBase = 1024 * 8;       # Convert into kb/s
                my $inBytes = $hash{rxbytes} - $lastValues->{rxbytes};
                $inBytes = 0 if $inBytes < 0;
                $hash{rxrate} = $inBytes / $timeDiff / $unitBase;
                my $outBytes = $hash{txbytes} - $lastValues->{txbytes};
                $outBytes = 0 if $outBytes < 0;
                $hash{txrate} = $outBytes / $timeDiff / $unitBase;
            }
        }
        $heap->{'network'}->{$card} = \%hash;
    }
    close $netdev;

    # Try to aggregate statistics per network
    delete @{ $heap->{'network'} }{ values %networkLookups };
    open my $netroute, '<', $routefile or die "Can't open $routefile: $!";
  LINE:
    while (<$netroute>) {
        next LINE if m/^ (?: Iface | $)/xms;    # Skip first and empty lines
        if ( my ( $card, $dest ) = m/^\s*(\w+)\s+(\w+)\s+/xms ) {
            next LINE if length($dest) != 8;
            my $classB = scalar reverse substr( $dest, 4, 4 );
            my $network = $networkLookups{$classB};
            next LINE if !defined $network;
            $heap->{'network'}->{$network}->{rxrate} +=
              $heap->{'network'}->{$card}->{rxrate} || 0;
            $heap->{'network'}->{$network}->{txrate} +=
              $heap->{'network'}->{$card}->{txrate} || 0;
        }
    }
    close $netroute;
    $kernel->delay( update_bandwidth => 60 );
}

sub update_client_id {
    my ( $self, $kernel, $heap ) = @_[ OBJECT, KERNEL, HEAP ];

    $kernel->yield(
        send => {
            'command' => 'GetID',
            'client'  => $self->{Name},
        }
    );
    $kernel->delay( update_client_id => 300 );    # Update ID every 5 minutes
}

sub get_client_id {
    my ( $kernel, $heap ) = @_[ KERNEL, HEAP ];

    my $clientID = $heap->{clientID};
    return $clientID if defined $clientID;

    # No client ID yet, get one and return -1
    $kernel->delay( 'update_client_id', 30 );
    return -1;
}

sub get_client_work {
    my ( $self, $heap, $kernel ) = @_[ OBJECT, HEAP, KERNEL ];

    $heap->{WorkRequested} = time;

    $kernel->yield(
        send => {
            command => 'SendClientWork',
            client  => $self->{Name},
        }
    );
}

sub get_work {
    my ( $self, $heap, $kernel ) = @_[ OBJECT, HEAP, KERNEL ];

    $heap->{WorkRequested} = time;

    $kernel->yield(
        send => {
            command => 'SendWork',
            client  => $self->{Name},
        }
    );
}

sub copy_done {
    my ( $self, $kernel, $heap, $hash_ref ) = @_[ OBJECT, KERNEL, HEAP, ARG0 ];

    $self->Verbose("Copy $hash_ref->{id} finished\n");
    if ( $hash_ref->{status} ) {
        for my $file ( @{ $hash_ref->{files} } ) {
            $self->Debug( "rfcp "
                  . $file->{source} . " "
                  . $file->{target}
                  . " returned "
                  . $hash_ref->{status}
                  . "\n" );
        }
    }

    $kernel->yield( 'job_done' => $hash_ref->{id} );
}

sub job_done {
    my ( $self, $heap, $kernel, $id ) = @_[ OBJECT, HEAP, KERNEL, ARG0 ];

    my $hash_ref = delete $heap->{HashRef}->{$id};
    return unless $hash_ref;

    my $queue_size  = keys %{ $heap->{HashRef} };
    my $queue_state = $queue_size ? ", still $queue_size to do" : '';
    my $status      = 'failed';
    if ( $hash_ref->{status} == 0 ) {
        $status = 'succeeded';

        # record copy time
        $hash_ref->{time} = time - $hash_ref->{work}->{WorkStarted};
    }
    $self->Verbose("Copy $id $status$queue_state\n");

    # send report back to manager
    $hash_ref->{client}  = $self->{Name};
    $hash_ref->{command} = 'JobDone';
    $kernel->yield( send => $hash_ref );

    # And request more work
    $kernel->yield('should_get_work');
}

sub handle_default {
    my ( $self, $kernel, $event, $args ) = @_[ OBJECT, KERNEL, ARG0, ARG1 ];
    $self->Verbose( "WARNING: Session "
          . $_[SESSION]->ID
          . " caught unhandled event $event with (@$args).\n" );
}

1;
