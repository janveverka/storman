use strict;
use warnings;
package T0::Copy::Worker;
use POE;
use POE::Filter::Reference;
use POE::Component::Client::TCP;
use Sys::Hostname;
use File::Basename;
use XML::Twig;
use T0::Util;
use T0::Castor::Rfcp;

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
				]
	],
  );

  return $self;
}

sub start_task {
  my ( $heap, $self ) = @_[ HEAP, ARG0 ];

  # keep reference to myself
  $heap->{Self} = $self;

  # put some parameters on heap
  $heap->{Node} = $self->{Node};

  #initialize some parameters
  $heap->{State} = 'Created';
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

      if ( $work->{SplitMode} eq 'streamerLFN' )
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
	  if (defined $work->{IndexDir})
	    {
	      $work->{IndexDir} .= $lfndir;
	    }

	  $work->{PFN} = $work->{TargetDir} . '/' . $work->{FILENAME};

	  $work->{LFN} = $lfndir . "/" . $work->{FILENAME};
	}
      elsif ( $work->{SplitMode} eq 'dataLFN' )
	{
	  my $run = $work->{RUNNUMBER};

	  my $lfndir;

	  if ( ( not defined($work->{STREAM})  ) or $work->{STREAM} eq '' )
	    {
	      $lfndir = sprintf("/store/data/%s/%03d/%03d/%03d", $work->{SETUPLABEL},
				$run/1000000, ($run%1000000)/1000, $run%1000);
	    }
	  else
	    {
	      $lfndir = sprintf("/store/data/%s/%s/%03d/%03d/%03d", $work->{SETUPLABEL}, $work->{STREAM},
				$run/1000000, ($run%1000000)/1000, $run%1000);
	    }

	  $work->{TargetDir} .= $lfndir;
	  if (defined $work->{IndexDir})
	    {
	      $work->{IndexDir} .= $lfndir;
	    }

	  $work->{PFN} = $work->{TargetDir} . '/' . $work->{FILENAME};

	  $work->{LFN} = $lfndir . "/" . $work->{FILENAME};
	}
      elsif ( $work->{SplitMode} eq 'lumiLFN' )
	{
	  my $lfndir = sprintf("/store/lumi/%04d%02d", $year, $month);

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
    # ask again in 10 seconds
    $kernel->delay_set( 'get_work', 10 );
    return;
  }

  if ( $command =~ m%DoThis% )
  {
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

    # send another message to logger (for MonaLisa)
    my %loghash = (
		   MonaLisa => 1,
		   Cluster => $T0::System{Name},
		   Node => $heap->{Node},
		   IdleTime => $heap->{WorkStarted} - $heap->{WorkRequested},
		  );
    $heap->{Self}->Log( \%loghash );

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
    push(@{ $rfcphash{files} }, { source => $sourcefile, target => $targetfile } );

    # check for file size
    my $filesize = qx{stat -L --format=%s $sourcefile};
    if ( int($work->{FILESIZE}) != int($filesize) )
      {
	$heap->{Self}->Quiet("file size on disk does not match size in notification\n");
	$heap->{HashRef}->{status} = -1;
	$kernel->yield('job_done');
	return;	
      }

    # check for index file
    if ( defined($work->{INDEX}) and $work->{TargetDir} ne '/dev/null' )
      {
	my $indexfile = $work->{PATHNAME} . '/' . $work->{INDEX};

	if (-s $indexfile)
	  {
	    $work->{INDEXSIZE} = qx{stat -L --format=%s $indexfile};

	    if (defined $work->{IndexDir})
	      {
		$work->{INDEXPFN} = $work->{IndexDir} . '/' . $work->{INDEX};
		#$work->{INDEXPFNBACKUP} = $work->{TargetDir} . '/' . $work->{INDEX};
		push(@{ $rfcphash{files} }, { source => $indexfile, target => $work->{INDEXPFN} } );
		#push(@{ $rfcphash{files} }, { source => $indexfile, target => $work->{INDEXPFNBACKUP} } );
	      }
	    else
	      {
		#$work->{INDEXPFN} = $work->{INDEXPFNBACKUP} = $work->{TargetDir} . '/' . $work->{INDEX};
		#push(@{ $rfcphash{files} }, { source => $indexfile, target => $work->{INDEXPFN} } );
	      }
	  }
	else
	  {
	    $heap->{Self}->Quiet("index file does not exist, is not a regular file or empty\n");
	    $heap->{HashRef}->{status} = -1;
	    $kernel->yield('job_done');
	    return;
	  }
      }

    $heap->{Self}->Debug("Copy " . $hash_ref->{id} . " started\n");

    T0::Castor::Rfcp->new(\%rfcphash);

    return;
  }

  if ( $command =~ m%Setup% )
  {
    $heap->{Self}->Quiet("Got $command...\n");
    my $setup = $hash_ref->{setup};
    $heap->{Self}->{Debug} && dump_ref($setup);
    map { $self->{$_} = $setup->{$_} } keys %$setup;

    if ( $heap->{State} eq 'Running' )
      {
	$kernel->yield('get_work');
      }
    return;
  }

  if ( $command =~ m%Start% )
  {
    $heap->{Self}->Quiet("Got $command...\n");
    if ( $heap->{State} eq 'Created' )
      {
	$heap->{State} = 'Running';
	$kernel->yield('get_work');
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

      # send message to logger
#      my %loghash1 = (
#		      CopyID => $heap->{HashRef}->{id},
#		      Status => $heap->{HashRef}->{status},
#		     );
#      $heap->{Self}->Log( \%loghash1 );

      # send another message to logger (for MonaLisa)
      my %loghash2 = (
		      MonaLisa => 1,
		      Cluster => $T0::System{Name},
		      Node => $heap->{Node},
		      CopyTime => $heap->{HashRef}->{time},
		     );
      $heap->{Self}->Log( \%loghash2 );
    }
  else
    {
      $heap->{Self}->Verbose("Copy " . $heap->{HashRef}->{id} . " failed\n");

      # send message to logger
#      my %loghash1 = (
#		      CopyID => $heap->{HashRef}->{id},
#		      Status => $heap->{HashRef}->{status},
#		     );
#      $heap->{Self}->Log( \%loghash1 );
    }

  # send report back to manager
  $heap->{HashRef}->{command} = 'JobDone';
  $self->send( $heap, $heap->{HashRef} );

  if ( ($heap->{State} eq 'Running') )
    {
      $kernel->yield('get_work');
    }
  else
    {
      Print "Shutting down...\n";
      $kernel->yield('shutdown');
      exit;
    }
}

1;
