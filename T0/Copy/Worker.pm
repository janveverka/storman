use strict;
use warnings;
package T0::Copy::Worker;
use POE;
use POE::Filter::Reference;
use POE::Component::Client::TCP;
use Sys::Hostname;
use File::Basename;
use T0::Util;
use T0::Castor::Rfcp;
use LWP::Simple;

our (@ISA, @EXPORT, @EXPORT_OK, %EXPORT_TAGS, $VERSION);

use Carp;
$VERSION = 1.00;
@ISA = qw/ Exporter /;

our $hdr = __PACKAGE__ . ':: ';
sub Croak   { croak $hdr,@_; }
sub Carp    { carp  $hdr,@_; }
sub Verbose { T0::Util::Verbose( (shift)->{Verbose}, @_ ); }
sub Debug   { T0::Util::Debug(   (shift)->{Debug},   @_ ); }
sub Quiet   { T0::Util::Quiet(   (shift)->{Quiet},   @_ ); }

sub new
{
  my $class = shift;
  my $self = {};
  bless($self, $class);

  my %h = @_;
  map { $self->{$_} = $h{$_} } keys %h;

  $self->ReadConfig();

  $self->{Host} = hostname();
  $self->{Name} .= '-' . $self->{Host} . '-' . $$;

  if ( defined($self->{Logger}) ) { $self->{Logger}->Name($self->{Name}); }

  POE::Component::Client::TCP->new
  ( RemotePort     => $self->{Manager}->{Port},
    RemoteAddress  => $self->{Manager}->{Host},
    Alias          => $self->{Name},
    Filter         => "POE::Filter::Reference",
    ConnectTimeout => 5,
    ServerError    => \&server_error,
    ConnectError   => \&_connection_error_handler,
    Disconnected   => \&_connection_error_handler,
    Connected      => \&_connected,
    ServerInput    => \&_server_input,
    Started        => \&start_task,
    Args => [ $self ],
    ObjectStates   => [
		       $self =>	[
				 server_input => 'server_input',
				 connected => 'connected',
				 connection_error_handler => 'connection_error_handler',
				 copy_done => 'copy_done',
				 job_done => 'job_done',
				 get_work => 'get_work',
				 get_client_work => 'get_client_work',
				 should_get_work => 'should_get_work',
				 get_client_id => 'get_client_id',
                 get_beam_status => 'get_beam_status',
                 get_bandwidth => 'get_bandwidth',
				 update_client_id => 'update_client_id',
                 update_beam_status => 'update_beam_status',
                 update_bandwidth => 'update_bandwidth',
                 sig_abort => 'sig_abort',
				]
	],
  );

  return $self;
}

# Some terminal signal got received
sub sig_abort {
    my ( $kernel, $heap, $signal ) = @_[ KERNEL, HEAP, ARG0 ];
    $kernel->Log( "Shutting down on signal SIG$signal" );
    $heap->{State} = 'Stop';
    $kernel->sig_handled();
}

sub start_task {
  my ( $heap, $self, $kernel ) = @_[ HEAP, ARG0, KERNEL ];

  # keep reference to myself
  $heap->{Self} = $self;

  # put some parameters on heap
  $heap->{Node} = $self->{Node};

  #initialize some parameters
  $heap->{State} = 'Created';

  #
  # check if rfcp version supports checksum
  # (default is off)
  #
  $heap->{UseRfcpChecksum} = 0;

  my $output = qx{castor -v 2>/dev/null};

  if ( $output )
  {
      my ($version,$subversion) = split('-',$output);
      my ($version1,$version2,$version3) = split('\.',$version);
      if ( ( $version1 > 2 ) or
	   ( $version1 == 2 and $version2 > 1 ) or
	   ( $version1 == 2 and $version2 == 1 and $version3 > 8 ) or
	   ( $version1 == 2 and $version2 == 1 and $version3 == 8 and $subversion >= 12 ) )
      {
	  $heap->{Self}->Quiet("Castor version check successfull, use preset checksums for rfcp transfers\n");
	  $heap->{UseRfcpChecksum} = 1;
      }
  }
  $kernel->yield( 'update_bandwidth' );   # Calculate bandwidth usage every minute
  $kernel->yield( 'update_beam_status' ); # Get beam status every minute

  # Try to shutdown nicely, that is letting copies finish
  $kernel->sig( INT  => 'sig_abort' );
  $kernel->sig( TERM => 'sig_abort' );
  $kernel->sig( QUIT => 'sig_abort' );
}

sub ReadConfig
{
  my $self = shift;
  my $file = $self->{Config};

  return unless $file;
  $self->Log("Reading configuration file ",$file);

  my $n = $self->{Name};
  $n =~ s%Worker.*$%Manager%;
  $self->{Partners} = { Manager => $n };
  $n = $self->{Name};
  $n =~ s%-.*$%%;
  T0::Util::ReadConfig( $self, , $n );

  map { $self->{Channels}->{$_} = 1; } @{$T0::System{Channels}}
}

sub server_error { Print $hdr," Server error\n"; }

sub _connection_error_handler { reroute_event( (caller(0))[3], @_ ); }
sub connection_error_handler
{
  my ( $self, $kernel ) = @_[ OBJECT, KERNEL ];

# return if $self->{OnError}(@_);

  my $retry = $self->{RetryInterval};
  defined($retry) && $retry>0 || return;

  if ( !$self->{Retries}++ )
  {
    Print $hdr," Connection retry every $retry seconds\n";
  }
  $kernel->delay( reconnect => $retry );
}

sub send
{
  my ( $self, $heap, $ref ) = @_;
  if ( !ref($ref) ) { $ref = \$ref; }
  if ( $heap->{connected} && $heap->{server} )
  {
    $heap->{server}->put( $ref );
  }
}

sub Log
{
  my $self = shift;
  my $logger = $self->{Logger};
  $logger->Send(@_) if defined $logger;
}

sub prepare_work
{
  my ($self, $work) = @_;

  # deciding what parameters set to apply according to the destination
  my $dsparams;
  if ( exists($work->{DESTINATION}) && exists($self->{DestinationConfiguration}->{$work->{DESTINATION}}) ){
    $dsparams = $self->{DestinationConfiguration}->{$work->{DESTINATION}};
  } else {
    $dsparams = $self->{DestinationConfiguration}->{default};
  };

  # feed parameters into work hash
  map{ $work->{$_} = $dsparams->{$_} } keys %$dsparams;

  if ( $work->{TargetDir} eq '/dev/null' )
    {
      $work->{PFN} = '/dev/null';
    }
  elsif ( defined($work->{SplitMode}) )
    {
      my ($day,$month,$year) = (localtime(time))[3,4,5];
      $month += 1;
      $year += 1900;

      $work->{InjectIntoTier0} = 0;

      if ( $work->{SplitMode} eq 'tier0StreamerNewPoolsLFN' )
	{
	  my $run = $work->{RUNNUMBER};

	  my $lfndir;

	  if ( ( not defined($work->{STREAM}) ) or $work->{STREAM} eq '' )
	    {
	      $lfndir = sprintf("/store/t0streamer/%s/%03d/%03d/%03d", $work->{SETUPLABEL},
				$run/1000000, ($run%1000000)/1000, $run%1000);
	    }
	  else
	    {
	      $lfndir = sprintf("/store/t0streamer/%s/%s/%03d/%03d/%03d", $work->{SETUPLABEL}, $work->{STREAM},
				$run/1000000, ($run%1000000)/1000, $run%1000);
	    }

	  $work->{TargetDir} .= $lfndir;

	  $work->{PFN} = $work->{TargetDir} . '/' . $work->{FILENAME};

	  $work->{LFN} = $lfndir . "/" . $work->{FILENAME};

	  $work->{InjectIntoTier0} = 1;
	}
      elsif ( $work->{SplitMode} eq 'tier0StreamerLFN' )
	{
	  my $run = $work->{RUNNUMBER};

	  my $lfndir;

	  if ( ( not defined($work->{STREAM}) ) or $work->{STREAM} eq '' )
	    {
	      $lfndir = sprintf("/store/streamer/%s/%03d/%03d/%03d", $work->{SETUPLABEL},
				$run/1000000, ($run%1000000)/1000, $run%1000);
	    }
	  else
	    {
	      $lfndir = sprintf("/store/streamer/%s/%s/%03d/%03d/%03d", $work->{SETUPLABEL}, $work->{STREAM},
				$run/1000000, ($run%1000000)/1000, $run%1000);
	    }

	  $work->{TargetDir} .= $lfndir;

	  $work->{PFN} = $work->{TargetDir} . '/' . $work->{FILENAME};

	  $work->{LFN} = $lfndir . "/" . $work->{FILENAME};

	  $work->{InjectIntoTier0} = 1;
	}
      elsif ( $work->{SplitMode} eq 'streamerLFN' )
	{
	  my $run = $work->{RUNNUMBER};

	  my $lfndir;

	  if ( ( not defined($work->{STREAM}) ) or $work->{STREAM} eq '' )
	    {
	      $lfndir = sprintf("/store/streamer/%s/%03d/%03d/%03d", $work->{SETUPLABEL},
				$run/1000000, ($run%1000000)/1000, $run%1000);
	    }
	  else
	    {
	      $lfndir = sprintf("/store/streamer/%s/%s/%03d/%03d/%03d", $work->{SETUPLABEL}, $work->{STREAM},
				$run/1000000, ($run%1000000)/1000, $run%1000);
	    }

	  $work->{TargetDir} .= $lfndir;

	  $work->{PFN} = $work->{TargetDir} . '/' . $work->{FILENAME};

	  $work->{LFN} = $lfndir . "/" . $work->{FILENAME};
	}
      elsif ( $work->{SplitMode} eq 'lumiLFN' )
      {
	  my $lfndir;

	  if ( $work->{FILENAME} =~ /CMS_LUMI_RAW_([0-9][0-9][0-9][0-9])([0-9][0-9])([0-9][0-9])_([0-9]+)_([^.]+).root/ ) {

	      $lfndir = sprintf("/store/lumi/%04d%02d", $1, $2);

	  } else {

	      $lfndir = sprintf("/store/lumi/%04d%02d", $year, $month);

	  }

	  $work->{TargetDir} .= $lfndir;

	  $work->{PFN} = $work->{TargetDir} . '/' . $work->{FILENAME};

	  $work->{LFN} = $lfndir . "/" . $work->{FILENAME};
	}
      elsif ( $work->{SplitMode} eq 'dqmLFN' )
	{
	  my $lfndir = sprintf("/store/temp/dqm/online/%04d%02d", $year, $month);

	  $work->{TargetDir} .= $lfndir;

	  $work->{PFN} = $work->{TargetDir} . '/' . $work->{FILENAME};

	  $work->{LFN} = $lfndir . "/" . $work->{FILENAME};
	}
      elsif ( $work->{SplitMode} eq 'daily' )
	{
	  my $temp = sprintf("/%04d%02d%02d", $year, $month, $day);

	  $work->{TargetDir} .= $temp;

	  $work->{PFN} = $work->{TargetDir} . '/' . $work->{FILENAME};
	}
      else
	{
	  $work->{PFN} = $work->{TargetDir} . '/' . $work->{FILENAME};
	}
    }
  #
  # Deprecated, remove all these lines and replace by
  # and else that prints out an error message
  #
  elsif ( $work->{CreateLFN} )
    {
      my $run = $work->{RUNNUMBER};

      my $lfndir = sprintf("/store/data/%s/%s/%03d/%03d/%03d", $work->{SETUPLABEL}, $work->{STREAM},
			   $run/1000000, ($run%1000000)/1000, $run%1000);

      $work->{TargetDir} .= $lfndir;

      $work->{PFN} = $work->{TargetDir} . '/' . $work->{FILENAME};

      $work->{LFN} = $lfndir . "/" . $work->{FILENAME};
    }
  elsif ( $work->{SplitByDay} )
    {
      my ($day,$month,$year) = (localtime(time))[3,4,5];

      $day = $day < 10 ? $day = "0".$day : $day;

      $month += 1;
      $month = $month < 10 ? $month = "0".$month : $month;

      $year += 1900;

      $work->{TargetDir} .= "/" . $year . $month . $day;

      $work->{PFN} = $work->{TargetDir} . '/' . $work->{FILENAME};
    }
  else
    {
      $work->{PFN} = $work->{TargetDir} . '/' . $work->{FILENAME};
    }
};


sub _server_input { reroute_event( (caller(0))[3], @_ ); }
sub server_input {
  my ( $self, $kernel, $heap, $session, $hash_ref ) = @_[ OBJECT, KERNEL, HEAP, SESSION, ARG0 ];

  my $command  = $hash_ref->{command};

  $heap->{Self}->Verbose("from server: $command\n");
  if ( $command =~ m%Sleep% )
  {
    # ask again in how many seconds the server said
    my $wait = $hash_ref->{wait} || 10;
    $heap->{Self}->Verbose("Sleeping $wait seconds, as ordered\n");
    $kernel->delay( should_get_work => $wait );
    return;
  }

  if ( $command =~ m%DoThis% )
  {
    $heap->{State} = 'Busy';

    $heap->{HashRef} = $hash_ref;

    my $work = $hash_ref->{work};

    # nothing went wrong yet
    $hash_ref->{status} = 0;

    # source file
    my $sourcefile = $work->{PATHNAME} . '/' . $work->{FILENAME};

    # configuring target dir, pfn and lfn
    prepare_work($self, $work);

    # target file
    my $targetfile = $work->{PFN};

    # mark start time
    $heap->{WorkStarted} = time;

    # set DeleteBadFiles option
    my $deletebadfiles;
    if(defined($work->{DeleteBadFiles}))
      {
	# coming from work hash
	$deletebadfiles = $work->{DeleteBadFiles};
      }
    else
      {
	# coming from config file (default option)
	$deletebadfiles = $heap->{Self}->{DeleteBadFiles};
      }

    my %rfcphash = (
		    svcclass => $work->{SvcClass},
		    session => $session,
		    callback => 'copy_done',
		    timeout => $work->{TimeOut},
		    retries => $work->{Retry},
  		    retry_backoff => $work->{RetryBackoff},
		    delete_bad_files => $deletebadfiles,
		    files => [],
		   );

    while ( my ($key, $value) = each(%{$work}) ) {
      if ( defined $value )
	{
	  $self->Debug("$key => $value\n");
	}
    }

    $heap->{Self}->Debug("Copy " . $hash_ref->{id} . " added " . basename($sourcefile) . "\n");
    push(@{ $rfcphash{files} }, { source => $sourcefile, target => $targetfile, checksum => $work->{CHECKSUM} } );

    # check source file for existance and file size
    my $filesize = qx{stat -L --format=%s $sourcefile 2> /dev/null};
    if ( ( $? != 0 ) or ( int($work->{FILESIZE}) != int($filesize) ) )
      {
	$heap->{Self}->Quiet("Source file does not exist or does not match file size in notification\n");
	$heap->{HashRef}->{status} = -1;
	$kernel->yield('job_done');
	return;	
      }

    $heap->{Self}->Debug("Copy " . $hash_ref->{id} . " started\n");

    T0::Castor::Rfcp->new(\%rfcphash, $heap->{UseRfcpChecksum});

    return;
  }

  if ( $command =~ m%Setup% )
  {
    $heap->{Self}->Quiet("Got $command...\n");
    my $setup = $hash_ref->{setup};
    $heap->{Self}->{Debug} && dump_ref($setup);
    map { $self->{$_} = $setup->{$_} } keys %$setup;

    if ( $heap->{State} eq 'Idle' )
      {
	$kernel->yield('should_get_work');
      }
    return;
  }

  if ( $command =~ m%Start% )
  {
    $heap->{Self}->Quiet("Got $command...\n");
    if ( $heap->{State} eq 'Created' )
      {
	$heap->{State} = 'Idle';
	$kernel->yield('should_get_work');
      }
    return;
  }

  if ( $command =~ m%Stop% )
  {
    $heap->{Self}->Quiet("Got $command...\n");
    $heap->{State} = 'Stop';
    return;
  }

  if ( $command =~ m%Quit% )
  {
    $heap->{Self}->Quiet("Got $command...\n");
    $heap->{State} = 'Quit';
    $kernel->yield('shutdown');
    return;
  }

  if ( $command =~ m/SetID/ ) {
    my $clientID = $hash_ref->{clientID};
    $heap->{Self}->Verbose("Got $command = $clientID\n");
    $heap->{clientID} = $clientID;
    $kernel->delay( update_client_id => 300 ); # Update ID every 5 minutes
    return;
  }

  Print "Error: unrecognised input from server! \"$command\"\n";
  $kernel->yield('shutdown');
}

sub _connected { reroute_event( (caller(0))[3], @_ ); }
sub connected
{
  my ( $self, $heap, $kernel, $hash_ref ) = @_[ OBJECT, HEAP, KERNEL, ARG0 ];
  $self->Debug("handle_connect: from server: $hash_ref\n");
  my %text = (  'command'       => 'HelloFrom',
                'client'        => $self->{Name},
		'hostname'      => $self->{Host},
             );
  $self->send( $heap, \%text );
}

sub should_get_work {
  my ( $self, $kernel, $session, $heap, $hash_ref ) = @_[ OBJECT, KERNEL, SESSION, HEAP, ARG0 ];

  my $delay = 60;
  my $minWorkers = $self->{MinWorkers} || 3;
  my $maxWorkers = $self->{MaxWorkers} || 99;
  my $extraWorkers = $self->{ExtraWorkers} || 0;
  my $id = $kernel->call( $session, 'get_client_id' );

  # Check if we're shutting down
  if( $heap->{State} eq 'Stop' ) {
    $kernel->yield('shutdown');
  }

  # Check we got a proper ID, otherwise sleep, hoping we'll get a better one
  if( $id < 1 or $id > 100 ) {
    $self->Verbose("WARNING: Got $id as id, not within 1..100! Retrying in $delay seconds\n");
    $kernel->delay( should_get_work => $delay );
    return;
  }

  # If our ID is less than the minimum number of workers, request work
  if( $id <= $minWorkers ) {
    $kernel->yield('get_work');
    return;
  }

  # Ensure we're not running too many workers
  if( $id > $maxWorkers ) {
    $self->Debug("We are ID $id, and maxWorkers is $maxWorkers. Sleeping 5 min.\n");
    $kernel->delay( should_get_work => 300 );
  }

  # Treat nodes 12 and 13 on rack 7 differently as they have smaller disks
  my $hostname = $self->{Host};
  if( $hostname =~ /^srv-c2c07-1[23]$/xmsio
    && $id <= $minWorkers + $extraWorkers ) {
    $self->Debug("We are ID $id on $hostname. Would have requested work as $id <= $minWorkers + $extraWorkers\n");
#    $kernel->yield('get_work');
#    return;
  }

  # Check run status
  my $beamStatus = $kernel->call( $session, 'get_beam_status' );
  if( $beamStatus =~ /^(?:SQUEEZE | ADJUST | STABLE | UNKNOWN )$/xms ) {
    $self->Debug("Not interfill, beam is in $beamStatus. Sleeping $delay seconds\n");
#    $kernel->delay( should_get_work => $delay );
#    return;
  }

  # Check the bandwidth usage
  my $inData = $kernel->call( $session, 'get_bandwidth', Data => 'rxrate' );
  my $outTier0 = $kernel->call( $session, 'get_bandwidth', Tier0 => 'txrate' );
  $self->Debug("We are ID $id on $hostname. Incoming bandwidth is $inData. Out: $outTier0\n");
  if( $inData < 90 ) {
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
  if( ! defined $content ) {
    $self->Log("Could not get beam status from Level-0. Probably not running.");
  }
  else {
    my ( $headers, $values ) = split /\n/, $content;
    $values =~ s/\[[^\]]*]//g;
    @level0{split/,/, $headers} = split /","/, $values;
  }
  my @lhc_modes = qw( MACHINE_MODE BEAM_MODE CLOCK_STABLE );
  $heap->{lhc}->{$_} = $level0{'LHC_' . $_} for @lhc_modes;
  $self->Debug('LHC status: '.join(', ',map{ "$_ => '$heap->{lhc}->{$_}'" } @lhc_modes),"\n");
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
  
  my $devfile   = '/proc/net/dev';
  my $routefile = '/proc/net/route';
  my %networkLookups = (
    A00B => 'Service',
    A03B => 'Data',
    A08B => 'Tier0',
  );
    
  my @columns;
  open my $netdev, '<', $devfile or die "Can't open $devfile: $!";
  LINE:
  while( <$netdev> ) {

#Inter-|   Receive                                                |  Transmit
    next LINE if /^Inter-/; # Skip first line

# face |bytes    packets errs drop fifo frame compressed multicast|bytes    packets errs drop fifo colls carrier compressed
    if( /\|/ ) {
      my @headers = split /\|/;
      shift @headers; # Discard 'face'
      push @columns, map { "rx$_" } split /\s+/, shift @headers; # Receive
      push @columns, map { "tx$_" } split /\s+/, shift @headers; # Transmit
      next LINE;
    }
    next LINE unless s/^\s*(\w+):\s*//; # Skip virtual interfaces eth0:1

    # We have a real interface, calculate the bandwidth in and out
    my $card = $1;
    my %hash = ( lastUpdate => time() );
    @hash{@columns} = split /\s+/;
    if( my $lastValues = delete $heap->{'network'}->{$card} ) {
      if( my $lastUpdate = $lastValues->{lastUpdate} ) {
        my $timeDiff = $hash{lastUpdate} - $lastUpdate;
        next LINE unless $timeDiff;
        my $unitBase = 1024 * 8; # Convert into kb/s
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
  while( <$netroute> ) {
    next LINE if m/^ (?: Iface | $)/xms; # Skip first and empty lines
    if( my( $card, $dest) = m/^\s*(\w+)\s+(\w+)\s+/xms ) {
      next LINE if length($dest) != 8;
      my $classB = scalar reverse substr($dest, 4, 4);
      my $network = $networkLookups{$classB};
      next LINE if !defined $network;
      $heap->{'network'}->{$network}->{rxrate} += $heap->{'network'}->{$card}->{rxrate} || 0;
      $heap->{'network'}->{$network}->{txrate} += $heap->{'network'}->{$card}->{txrate} || 0;
    }
  }
  close $netroute;
  $kernel->delay( update_bandwidth => 60 );
}

sub update_client_id {
  my ( $self, $kernel, $heap ) = @_[ OBJECT, KERNEL, HEAP ];

  my $text = {
	      'command' => 'GetID',
	      'client'  => $self->{Name},
	     };
  $self->send( $heap, $text );
  $kernel->delay( update_client_id => 300 ); # Update ID every 5 minutes
}

sub get_client_id {
  my ( $kernel, $heap ) = @_[ KERNEL, HEAP ];

  my $clientID = $heap->{clientID};
  return $clientID if defined $clientID;

  # No client ID yet, get one and return -1
  $kernel->delay( 'update_client_id', 30 );
  return -1;
}

sub get_client_work
{
  my ( $self, $heap ) = @_[ OBJECT, HEAP ];

  $heap->{WorkRequested} = time;

  $self->send( $heap, { 'command' => 'SendClientWork',
	      'client' => $self->{Name}, } );
}

sub get_work
{
  my ( $self, $heap ) = @_[ OBJECT, HEAP ];

  $heap->{WorkRequested} = time;

  my %text = (
	      'command'      => 'SendWork',
	      'client'       => $self->{Name},
	     );
  $self->send( $heap, \%text );
}

sub copy_done {
  my ( $kernel, $heap, $hash_ref ) = @_[ KERNEL, HEAP, ARG0 ];

  foreach my $file ( @{ $hash_ref->{files} } )
    {
      if ( $file->{status} != 0 )
	{
	  $heap->{Self}->Debug("rfcp " . $file->{source} . " " . $file->{target} . " returned " . $file->{status} . "\n");
	  $heap->{HashRef}->{status} = $file->{status};
	  last;
	}
    }

  $kernel->yield('job_done');
}

sub job_done
{
  my ( $self, $heap, $kernel ) = @_[ OBJECT, HEAP, KERNEL ];

  if ( $heap->{HashRef}->{status} == 0 )
    {
      $heap->{Self}->Verbose("Copy " . $heap->{HashRef}->{id} . " succeeded\n");

      # record copy time
      $heap->{HashRef}->{time} = time - $heap->{WorkStarted};
    }
  else
    {
      $heap->{Self}->Verbose("Copy " . $heap->{HashRef}->{id} . " failed\n");
    }

  # send report back to manager
  $heap->{HashRef}->{command} = 'JobDone';
  $self->send( $heap, $heap->{HashRef} );

  if ( ($heap->{State} eq 'Busy') )
    {
      $heap->{State} = 'Idle';
      $kernel->yield('should_get_work');
    }
  else
    {
      Print "Shutting down...\n";
      $kernel->yield('shutdown');
      exit;
    }
}

1;
