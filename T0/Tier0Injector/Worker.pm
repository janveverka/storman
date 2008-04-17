use strict;
use warnings;
package T0::Tier0Injector::Worker;
use POE;
use POE::Filter::Reference;
use POE::Component::Client::TCP;
use Sys::Hostname;
use T0::Util;
use DBI;

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
  $heap->{DatabaseInstance} = $self->{DatabaseInstance};
  $heap->{DatabaseUser} = $self->{DatabaseUser};
  $heap->{DatabasePassword} = $self->{DatabasePassword};

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
    # save hash reference on heap, needed in job_done
    $heap->{HashRef} = $hash_ref;

    # convinience variable
    my $work = $hash_ref->{work};

    # mark start time
    $heap->{WorkStarted} = time;

    my $checksum = $work->{CHECKSUM};
    my $type = $work->{TYPE};
    my $dataset = $work->{DATASET};

    # nothing has gone wrong yet
    $hash_ref->{status} = 0;

    #
    # Check database handle, connect if needed
    #
    unless ( $heap->{DatabaseHandle} )
      {
	$heap->{DatabaseHandle} = DBI->connect($heap->{DatabaseInstance},$heap->{DatabaseUser},$heap->{DatabasePassword});
      }

    if ( $heap->{DatabaseHandle} )
      {
	my ($sql,$sth,$count);

	#
	# check if run is already in database
	#
	$count = 0;
	$sql = "select RUN_ID from CMS_T0AST_DIRK.run where run_id = ?";

	if ( $sth = $heap->{DatabaseHandle}->prepare_cached($sql) )
	  {
	    $sth->bind_param(1,int($work->{RUNNUMBER}));
	    if ( $sth->execute() )
	      {
		while ( my ($tmp) = $sth->fetchrow_array() )
		  {
		    ++$count;
		  }
	      }
	    else
	      {
		$heap->{Self}->Quiet("failed execute : $heap->{DatabaseHandle}->errstr\n");
		$hash_ref->{status} = 1;
	      }
	  }
	else
	  {
	    $heap->{Self}->Quiet("failed prepare : $heap->{DatabaseHandle}->errstr\n");
	    $hash_ref->{status} = 1;
	  }

	#
	# insert run if needed
	#
	if ( $hash_ref->{status} ==  0 and $count == 0 )
	  {
	    # FIXME : should be initialised with T0AST DB schema install
	    #         remove asap, that's why no error handling
	    $sql = "insert into CMS_T0AST_DIRK.run_status (STATUS_ID, RUN_STATUS) values (0,'open')";
	    $sth = $heap->{DatabaseHandle}->prepare( $sql );
	    $sth->execute();

	    $sql = "insert into CMS_T0AST_DIRK.run (RUN_ID, START_TIME, END_TIME, APP_NAME, APP_VERSION, RUN_STATUS) values (?,?,?,?,?,?)";

	    if ( $sth = $heap->{DatabaseHandle}->prepare_cached( $sql ) )
	      {
		$sth->bind_param(1,int($work->{RUNNUMBER}));
                $sth->bind_param(2,int($work->{START_TIME}));
                $sth->bind_param(3,int($work->{STOP_TIME}));
                $sth->bind_param(4,$work->{APP_NAME});
                $sth->bind_param(5,$work->{APP_VERSION});
                $sth->bind_param(6,int(0));
		if ( $sth->execute() )
		  {
		    $heap->{Self}->Quiet("Inserted run $work->{RUNNUMBER}\n");
		  }
		else
		  {
		    $heap->{Self}->Quiet("failed execute : $heap->{DatabaseHandle}->errstr\n");
		    $hash_ref->{status} = 1;
		  }
	      }
	    else
	      {
		$heap->{Self}->Quiet("failed prepare : $heap->{DatabaseHandle}->errstr\n");
		$hash_ref->{status} = 1;
	      }
	  }

	#
	# check if lumi section is already in database
	#
	if ( $hash_ref->{status} ==  0 )
	  {
	    $count = 0;
	    $sql = "select LUMI_ID, RUN_ID from CMS_T0AST_DIRK.lumi_section where lumi_id = ? and run_id = ?";

	    if ( $sth = $heap->{DatabaseHandle}->prepare_cached( $sql ) )
	      {
		$sth->bind_param(1,int($work->{LUMISECTION}));
		$sth->bind_param(2,int($work->{RUNNUMBER}));
		if ( $sth->execute() )
		  {
		    while ( my ($tmp1,$tmp2) = $sth->fetchrow_array() )
		      {
			++$count;
		      }
		  }
		else
		  {
		    $heap->{Self}->Quiet("failed execute : $heap->{DatabaseHandle}->errstr\n");
		    $hash_ref->{status} = 1;
		  }
	      }
	    else
	      {
		$heap->{Self}->Quiet("failed prepare : $heap->{DatabaseHandle}->errstr\n");
		$hash_ref->{status} = 1;
	      }
	  }

	#
	# insert lumi section if needed
	#
	if ( $hash_ref->{status} ==  0 and $count == 0 )
	  {
	    $sql = "insert into CMS_T0AST_DIRK.lumi_section (LUMI_ID, RUN_ID, START_TIME) values (?,?,?)";

	    if ( $sth = $heap->{DatabaseHandle}->prepare_cached( $sql ) )
	      {
		$sth->bind_param(1,int($work->{LUMISECTION}));
		$sth->bind_param(2,int($work->{RUNNUMBER}));
		$sth->bind_param(3,int($work->{START_TIME}));
		if ( $sth->execute() )
		  {
		    $heap->{Self}->Quiet("Inserted lumi section $work->{LUMISECTION} for run $work->{RUNNUMBER}\n");
		  }
		else
		  {
		    $heap->{Self}->Quiet("failed execute : $heap->{DatabaseHandle}->errstr\n");
		    $hash_ref->{status} = 1;
		  }
	      }
	    else
	      {
		$heap->{Self}->Quiet("failed prepare : $heap->{DatabaseHandle}->errstr\n");
		$hash_ref->{status} = 1;
	      }
	  }

	#
	# check if streamer is already in database
	#
	if ( $hash_ref->{status} ==  0 )
	  {
	    $count = 0;
	    $sql = "select LFN from CMS_T0AST_DIRK.streamer where lfn = ?";

	    if ( $sth = $heap->{DatabaseHandle}->prepare_cached( $sql ) )
	      {
		$sth->bind_param(1,$work->{LFN});
		if ( $sth->execute() )
		  {
		    while ( my ($tmp) = $sth->fetchrow_array() )
		      {
			++$count;
		      }
		  }
		else
		  {
		    $heap->{Self}->Quiet("failed execute : $heap->{DatabaseHandle}->errstr\n");
		    $hash_ref->{status} = 1;
		  }
	      }
	    else
	      {
		$heap->{Self}->Quiet("failed prepare : $heap->{DatabaseHandle}->errstr\n");
		$hash_ref->{status} = 1;
	      }
	  }

	#
	# insert streamer if needed
	#
	if ( $hash_ref->{status} ==  0 and $count == 0 )
	  {
	    $sql = "insert into CMS_T0AST_DIRK.streamer (LUMI_ID, RUN_ID, START_TIME, FILESIZE, EVENTS, LFN, STREAMNAME, EXPORTABLE, SPLITABLE, INDEXPFN, INDEXPFNBACKUP) values (?,?,?,?,?,?,?,?,?,?,?)";

	    if ( $sth = $heap->{DatabaseHandle}->prepare_cached( $sql ) )
	      {
		$sth->bind_param(1,int($work->{LUMISECTION}));
		$sth->bind_param(2,int($work->{RUNNUMBER}));
		$sth->bind_param(3,int($work->{START_TIME}));
		$sth->bind_param(4,int($work->{FILESIZE}));
		$sth->bind_param(5,int($work->{NEVENTS}));
		$sth->bind_param(6,$work->{LFN});
		$sth->bind_param(7,$work->{STREAM});
		$sth->bind_param(8,int(0));
		$sth->bind_param(9,int(1));
		$sth->bind_param(10,$work->{INDEXPFN});
		$sth->bind_param(11,$work->{INDEXPFNBACKUP});
		if ( $sth->execute() )
		  {
		    $heap->{Self}->Quiet("Inserted streamer with LFN $work->{LFN}\n");
		  }
		else
		  {
		    $heap->{Self}->Quiet("failed execute : $heap->{DatabaseHandle}->errstr\n");
		    $hash_ref->{status} = 1;
		  }
	      }
	    else
	      {
		$heap->{Self}->Quiet("failed prepare : $heap->{DatabaseHandle}->errstr\n");
		$hash_ref->{status} = 1;
	      }
	  }
	elsif ( $hash_ref->{status} ==  0 and $count > 0 )
	  {
	    $heap->{Self}->Quiet("Streamer with LFN $work->{LFN} already exists\n");
	    $hash_ref->{status} = 1;
	  }
      }
    else
      {
	$heap->{Self}->Quiet("failed connection to database\n");
	$hash_ref->{status} = 1;
      }

    if ( $hash_ref->{status} == 0 )
      {
	$heap->{Self}->Quiet("Injection for ", $work->{LFN}, " succeeded\n");
      }
    else
      {
	$heap->{Self}->Quiet("Injection for ", $work->{LFN}, " failed\n");
      }

    $kernel->yield('job_done');

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


sub job_done
{
  my ( $self, $heap, $kernel ) = @_[ OBJECT, HEAP, KERNEL ];

  # send report back to manager
  $heap->{HashRef}->{command} = 'JobDone';
  $self->send( $heap, $heap->{HashRef} );

  #delete $heap->{HashRef};

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
