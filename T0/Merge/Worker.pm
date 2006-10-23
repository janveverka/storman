use strict;
use warnings;
package T0::Merge::Worker;
use POE;
use POE::Filter::Reference;
use POE::Component::Client::TCP;
use Sys::Hostname;
use File::Basename;
use XML::Twig;
use T0::Util;
use T0::Copy::Rfcp;
use T0::Merge::FastMerge;

our (@ISA, @EXPORT, @EXPORT_OK, %EXPORT_TAGS, $VERSION);

use Carp;
$VERSION = 1.00;
@ISA = qw/ Exporter /;

#$Merge::Name = 'Merge::Worker-' . hostname();

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

  #$self->{Name} = $Merge::Name . '-' . $$;
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
				 rfcp_done => 'rfcp_done',
				 merge_done => 'merge_done',
				 stageout_done => 'stageout_done',
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
  $heap->{LogDir} = $self->{LogDir};
  $heap->{MaxTasks} = $self->{MaxTasks};
  $heap->{Node} = $self->{Node};
  $heap->{Mode} = $self->{Mode};

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
    $heap->{HashRef} = $hash_ref;

    # nothing went wrong yet
    $hash_ref->{status} = 0;

    # mark start time
    $heap->{WorkStarted} = time;

    # we got a new task
    $heap->{MaxTasks}--;

    # not used yet
    my $priority = defined $hash_ref->{priority} ? $hash_ref->{priority} : 99;

    # keep list of local temporary files for each merge job
    $heap->{outputfile} = 'merge_' . $hash_ref->{id} . '.root';
    $heap->{jobreport} = 'merge_' . $hash_ref->{id} . '.jobreport';
    $heap->{outputcatalog} = 'merge_' . $hash_ref->{id} . '.outputcatalog';
    my @temporaryFiles = ();
    push(@temporaryFiles, $heap->{outputfile}, $heap->{jobreport}, $heap->{outputcatalog} , $heap->{outputcatalog} . '.BAK');
    $heap->{TemporaryFiles} = \@temporaryFiles;

    # derive target directory from input
    if ( scalar @{$hash_ref->{work}} > 0 )
      {
	$hash_ref->{inputevents} = 0;

	# derive a few information from input
	foreach my $w ( @{$hash_ref->{work}} )
	  {
	    $hash_ref->{Dataset} = $w->{Dataset} unless defined $hash_ref->{Dataset};
	    $hash_ref->{Version} = $w->{Version} unless defined $hash_ref->{Version};
	    $hash_ref->{PsetHash} = $w->{PsetHash} unless defined $hash_ref->{PsetHash};
	    $hash_ref->{DataType} = $w->{DataType} unless defined $hash_ref->{DataType};
	    $hash_ref->{Stream} = $w->{Stream} unless defined $hash_ref->{Stream};

	    $hash_ref->{inputevents} += $w->{NbEvents};

	    unless ( defined $hash_ref->{TargetDir} )
	      {
		$hash_ref->{TargetDir} = dirname( $w->{PFNs} );
		$hash_ref->{TargetDir} =~ s%unmerged%%;
		$hash_ref->{TargetDir} =~ s%//%/%g;
	      }

	    # extract SvcClass (assumes they are the same for all input files)
	    # to fix this Rfcp.pm needs to be changed to allow for different SvcClass
	    # direct FastMerge.pm merging doesn't support multiple SvcClass
	    $hash_ref->{InputSvcClass} = $w->{SvcClass} unless defined $hash_ref->{InputSvcClass};
	    $hash_ref->{OutputSvcClass} = $w->{SvcClass} unless defined $hash_ref->{OutputSvcClass};
	  }

	# send message to logger
	my %loghash1 = (
			ForceMerge => '0',
			MergeID => $hash_ref->{id},
			work => [],
		       );
	foreach my $w ( @{$hash_ref->{work}} )
	  {
	    my %workhash = (
			    PFNs => $w->{PFNs},
			    NbEvents => $w->{NbEvents},
			    SvcClass => $w->{SvcClass},
			    Dataset => $w->{Dataset},
			    Version => $w->{Version},
			    PsetHash => $w->{PsetHash},
			    DataType => $w->{DataType},
			    Stream => $w->{Stream},
			    Parent => $w->{Parent},
			   );
	    push (@{$loghash1{work}}, \%workhash);
	  }
	$heap->{Self}->Log( \%loghash1 );

	# send another message to logger (for MonaLisa)
	my %loghash2 = (
			MonaLisa => 1,
			Cluster => $T0::System{Name},
			Node => $heap->{Node},
			IdleTime => $heap->{WorkStarted} - $heap->{WorkRequested},
		       );
	$heap->{Self}->Log( \%loghash2 );
      }
    else
      {
	$hash_ref->{status} = 999;
	$hash_ref->{reason} = 'no input files';
	$kernel->yield('job_done');
      }

    my %rfcphash = (
		    svcclass => $hash_ref->{InputSvcClass},
		    session => $session,
		    callback => 'rfcp_done',
		    timeout => 3600,
		    retries => 2,
		    files => []
		   );

    my %mergehash = (
		     svcclass => $hash_ref->{InputSvcClass},
		     session => $session,
		     callback => 'merge_done',
		     timeout => 3600,
		     retries => 1,
		     sources => [],
		     target => 'file://' . $heap->{outputfile},
		     jobreport => $heap->{jobreport},
		     outputcatalog => $heap->{outputcatalog},
		    );

    if ( $heap->{Mode} eq 'LocalPull' )
      {
	# create hashes for copy and merge
	foreach my $w ( @{$hash_ref->{work}} )
	  {
	    push(@{ $rfcphash{files} }, { source => $w->{PFNs}, target => '.' } );

	    $heap->{Self}->Debug("Add source file://" . basename($w->{PFNs}) . "\n");

	    push(@{ $mergehash{sources} }, 'file://' . basename($w->{PFNs}) );
	    push(@temporaryFiles, basename($w->{PFNs}));
	  }

	$heap->{MergeHash} = \%mergehash;
	$heap->{Self}->Debug("Copy started\n");

	T0::Copy::Rfcp->new(\%rfcphash);
      }
    elsif ( $heap->{Mode} eq 'Classic' )
      {
	# create hash for direct merge
	foreach my $w ( @{$hash_ref->{work}} )
	  {
	    $heap->{Self}->Debug("Add source rfio:" . $w->{PFNs} . "\n");

	    push(@{ $mergehash{sources} }, 'rfio:' . $w->{PFNs} );
	  }

	$heap->{Self}->Debug("Merge started\n");
	T0::Merge::FastMerge->new(\%mergehash);
      }
    elsif ( $heap->{Mode} eq 'LocalPush' )
      {
	# almost like LocalPull, except all
	# or most of the input files are
	# available locally

	# loop though input files, if local
	# just convert to file: syntax w/o copy
	# if not local, add to rfcp copy

	# might be a good idea to always pass
	# both PFN and WNlocation to the logger
	# that way Manager doesn't need to care
	# about Mode

	# how to handle existence check on
	# lxbXXXX:/blah files ? Easy for local
	# ones, but too much overhead for remote ?

	die "LocalPush not yet supported!\n";
      }

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

sub rfcp_done {
  my ( $kernel, $heap, $hash_ref ) = @_[ KERNEL, HEAP, ARG0 ];

  $heap->{Self}->Debug("Copy done\n");

  my $status = 0;
  foreach my $file ( @{ $hash_ref->{files} } )
    {
      if ( $file->{status} != 0 )
	{
	  $heap->{Self}->Debug("rfcp " . $file->{source} . " " . $file->{target} . " returned " . $file->{status} . "\n");
	  $status = $file->{status};
	  last;
	}
    }

  if ( $status != 0 )
    {
      $heap->{HashRef}->{status} = $status;
      $heap->{HashRef}->{reason} = 'stagein failed';
      $kernel->yield('job_done');
      return;
    }

  $heap->{Self}->Debug("Merge started\n");
  T0::Merge::FastMerge->new($heap->{MergeHash});
}

sub merge_done {
  my ( $kernel, $heap, $session, $hash_ref ) = @_[ KERNEL, HEAP, SESSION, ARG0 ];

  $heap->{Self}->Debug("Merge done\n");

  if ( $hash_ref->{status} != 0 )
    {
      $heap->{HashRef}->{status} = $hash_ref->{status};
      $heap->{HashRef}->{reason} = 'merge failed';
      $kernel->yield('job_done');
      return;
    }

  # record checksum and size of outputfile
  my @temp = split( ' ', qx { cksum $heap->{outputfile} } );
  $heap->{HashRef}->{checksum} = $temp[0];
  $heap->{HashRef}->{size} = $temp[1];

  #
  # Kludge: get events and size from EdmCollUtil
  #
  my $edmCollUtilOutput = qx {EdmCollUtil --file file://$heap->{outputfile} 2> /dev/null};

  if ( $edmCollUtilOutput =~ m%($heap->{outputfile}) \( (\d+) events, (\d+) bytes \)%g )
    {
      $heap->{HashRef}->{events} = $2;
    }

  # compare to input events
  # just print out warning for now
  if ( $heap->{HashRef}->{events} != $heap->{HashRef}->{inputevents} )
    {
      $heap->{HashRef}->{status} = 999;
      $heap->{HashRef}->{reason} = 'wrong event count, expected ' . $heap->{HashRef}->{inputevents} . ', got ' . $heap->{HashRef}->{events};
      $kernel->yield('job_done');
      return;
    }

  #
  # parse job report and output catalog
  #
  my $guid = undef;
  my $twig = XML::Twig->new(
			    twig_roots   => {
					     #'TotalEvents' => sub { my ($t,$elt)= @_; $heap->{HashRef}->{events} = $elt->text; $t->purge; },
					     'File' => sub { my ($t,$elt)= @_; $guid = $elt->{'att'}->{'ID'}; $t->purge; },
					    }
			   );
  #$twig->parsefile( $heap->{jobreport} );
  $twig->parsefile( $heap->{outputcatalog} );

  if ( not defined $guid )
    {
      $heap->{HashRef}->{status} = 999;
      $heap->{HashRef}->{reason} = 'could not extract GUID from merge $heap->{outputcatalog}';
      $kernel->yield('job_done');
      return;
    }

  my %rfcphash = (
		  svcclass => $heap->{HashRef}->{OutputSvcClass},
		  session => $session,
		  callback => 'stageout_done',
		  timeout => 3600,
		  retries => 3,
		  files => []
		 );

  $heap->{StageoutOutputfile} = $heap->{HashRef}->{TargetDir} . '/' . $guid . '.root';
  push(@{ $rfcphash{files} }, { source => $heap->{outputfile}, target => $heap->{StageoutOutputfile} } );
  if ( defined $heap->{LogDir} )
    {
      $heap->{StageoutJobReport} = $heap->{LogDir} . '/' . 'FrameworkJobReport.' . $guid . '.xml';
      push(@{ $rfcphash{files} }, { source => $heap->{jobreport}, target => $heap->{StageoutJobReport} } );
    }
  else
    {
      $heap->{StageoutJobReport} = '';
    }

  $heap->{Self}->Debug("Stageout started\n");
  T0::Copy::Rfcp->new(\%rfcphash);
}

sub stageout_done {
  my ( $kernel, $heap, $hash_ref ) = @_[ KERNEL, HEAP, ARG0 ];

  $heap->{Self}->Debug("Stageout done\n");

  my $status = 0;
  foreach my $file ( @{ $hash_ref->{files} } )
    {
      if ( $file->{status} != 0 )
	{
	  $heap->{Self}->Debug("rfcp " . $file->{source} . " " . $file->{target} . " returned " . $file->{status} . "\n");
	  if ( $file->{source} eq $heap->{jobreport} )
	    {
	      $heap->{Self}->Debug("Ignore stageout failure for jobreport\n");
	    }
	  else
	    {
	      $status = $file->{status};
	      last;
	    }
	}
    }

  if ( $status != 0 )
    {
      $heap->{HashRef}->{status} = $status;
      $heap->{HashRef}->{reason} = 'stageout failed';
    }

  $kernel->yield('job_done');
}

sub job_done
{
  my ( $self, $heap, $kernel ) = @_[ OBJECT, HEAP, KERNEL ];

  # remove local files
  foreach my $file ( @{$heap->{TemporaryFiles}} )
    {
      if ( $heap->{HashRef}->{status} == 0 )
	{
	  unlink($file);
	}
    }
  delete $heap->{TemporaryFiles};
  delete $heap->{outputfile};
  delete $heap->{jobreport};
  delete $heap->{outputcatalog};

  if ( $heap->{HashRef}->{status} == 0 )
    {
      $heap->{Self}->Verbose("Merge with ID " . $heap->{HashRef}->{id} . " succeeded\n");
      $heap->{Self}->Verbose("Events = " . $heap->{HashRef}->{events} . " , Size = " . $heap->{HashRef}->{size} . "\n");

      # record merge time
      $heap->{HashRef}->{time} = time - $heap->{WorkStarted};

      # record output file
      $heap->{HashRef}->{mergefile} = $heap->{StageoutOutputfile};

      # send message to logger
      my %loghash1 = (
		      MergeID => $heap->{HashRef}->{id},
		      OutputFile => $heap->{StageoutOutputfile},
		      JobReport => $heap->{StageoutJobReport},
		      Events => $heap->{HashRef}->{events},
		      Size => $heap->{HashRef}->{size},
		     );
      $heap->{Self}->Log( \%loghash1 );

      # send another message to logger (for MonaLisa)
      my %loghash2 = (
		      MonaLisa => 1,
		      Cluster => $T0::System{Name},
		      Node => $heap->{Node},
		      Events => $heap->{HashRef}->{events},
		      Size => $heap->{HashRef}->{size},
		      MergeTime => $heap->{HashRef}->{time},
		     );
      $heap->{Self}->Log( \%loghash2 );
    }
  else
    {
      $heap->{Self}->Verbose("Merge " . $heap->{HashRef}->{id} . " failed\n");
      $heap->{Self}->Verbose("Status = " . $heap->{HashRef}->{status} . " , Reason = \'" . $heap->{HashRef}->{reason} . "\'\n");

      # send message to logger
      my %loghash1 = (
		      MergeID => $heap->{HashRef}->{id},
		      Status => $heap->{HashRef}->{status},
		      Reason => $heap->{HashRef}->{reason}
		     );
      $heap->{Self}->Log( \%loghash1 );
    }

  # send report back to manager
  $heap->{HashRef}->{command} = 'JobDone';
  $self->send( $heap, $heap->{HashRef} );

  if ( ($heap->{State} eq 'Running') and ($heap->{MaxTasks} > 0) )
    {
      $heap->{Self}->Verbose("Tasks remaining: ",$heap->{MaxTasks},"\n");
      $kernel->yield('get_work');
    }
  else
    {
      Print "Shutting down...\n";
      $kernel->yield('shutdown');
    }
}

1;
