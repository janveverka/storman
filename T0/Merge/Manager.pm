use strict;
package T0::Merge::Manager;
use Sys::Hostname;
use POE;
use POE::Filter::Reference;
use POE::Component::Server::TCP;
use POE::Queue::Array;
use T0::Util;
use T0::FileWatcher;

our (@ISA, @EXPORT, @EXPORT_OK, %EXPORT_TAGS, $VERSION);

use Carp;
$VERSION = 1.00;
@ISA = qw/ Exporter /;
$Merge::Name = 'Merge::Manager';

our (@queue,%q);

our $hdr = __PACKAGE__ . ':: ';
sub Croak   { croak $hdr,@_; }
sub Carp    { carp  $hdr,@_; }
sub Verbose { T0::Util::Verbose( (shift)->{Verbose}, @_ ); }
sub Debug   { T0::Util::Debug(   (shift)->{Debug},   @_ ); }
sub Quiet   { T0::Util::Quiet(   (shift)->{Quiet},   @_ ); }

my %mergePendingWork;

sub _init
{
  my $self = shift;

  $self->{Name} = $Merge::Name;
  my %h = @_;
  map { $self->{$_} = $h{$_}; } keys %h;
  $self->ReadConfig();
  check_host( $self->{Host} ); 

#  Croak "undefined Application\n"
#    unless defined $self->{Application};

  Croak "no merge threshold defined\n"
    unless ( defined $self->{FileThreshold} or defined $self->{EventThreshold}
	     or defined $self->{SizeThreshold} or defined $self->{AgeThreshold} );

  POE::Component::Server::TCP->new
      (
       Port                => $self->{Port},
       Alias               => $self->{Name},
       ClientFilter        => "POE::Filter::Reference",
       ClientInput         => \&_client_input,
       ClientDisconnected  => \&_client_disconnected,
       ClientError         => \&_client_error,
       Started             => \&start_task,
       ObjectStates	=> [
			    $self => [
				      client_input	=> 'client_input',
				      client_error	=> 'client_error',
				      client_disconnected	=> 'client_disconnected',
				      handle_unfinished => 'handle_unfinished',
				      send_work => 'send_work',
				      send_setup => 'send_setup',
				      send_start => 'send_start',
				      file_changed => 'file_changed',
				      broadcast	=> 'broadcast',
				      MergeIsPending => 'MergeIsPending',
				      merge_timeout => 'merge_timeout',
				      merge_submit => 'merge_submit',
				     ],
			   ],
       Args => [ $self ],
      );

  $self->{Queue} = POE::Queue::Array->new();
  return $self;
}

sub new
{
  my $proto  = shift;
  my $class  = ref($proto) || $proto;
  my $parent = ref($proto) && $proto;
  my $self = {  };
  bless($self, $class);
  $self->_init(@_);
}

sub Options
{
  my $self = shift;
  my %h = @_;
  map { $self->{$_} = $h{$_}; } keys %h;
}

#our @attrs = ( qw/ Name Host Port / );
#our %ok_field;
#for my $attr ( @attrs ) { $ok_field{$attr}++; }

#sub AUTOLOAD {
#  my $self = shift;
#  my $attr = our $AUTOLOAD;
#  $attr =~ s/.*:://;
#  return unless $attr =~ /[^A-Z]/;  # skip DESTROY and all-cap methods
#  Croak "AUTOLOAD: Invalid attribute method: ->$attr()" unless $ok_field{$attr};
#  if ( @_ ) { Croak "Setting attributes not yet supported!\n"; }
# $self->{$attr} = shift if @_;
#  return $self->{$attr};
#}

sub start_task
{
  my ( $kernel, $heap, $session, $self ) = @_[ KERNEL, HEAP, SESSION, ARG0 ];
  my %param;

  # save reference to myself on heap
  $heap->{Self} = $self;

  # initalize some parameters
  $heap->{TotalEvents} = 0;
  $heap->{TotalVolume} = 0;

  $self->Debug($self->{Name}," has started...\n");
  $self->Log($self->{Name}," has started...\n");
  $self->{Session} = $session->ID;

  $kernel->state( 'send_setup', $self );
  $kernel->state( 'file_changed', $self );
  $kernel->state( 'broadcast', $self );
# _WHY_ do I need  to do this...?
  $kernel->state( 'MergeIsPending', $self );
  $kernel->state( 'merge_timeout', $self );
  $kernel->state( 'merge_submit', $self );

  # check age of files to be merged
  if ( exists $heap->{Self}->{AgeThreshold} )
    {
      $kernel->delay_set('merge_timeout',300);
    }

  %param = ( File     => $self->{Config},
             Interval => $self->{ConfigRefresh},
             Client   => $self->{Name},
             Event    => 'file_changed',
           );
 $self->{Watcher} = T0::FileWatcher->new( %param );
#  $kernel->yield( 'file_changed' );
}

sub merge_timeout {
  my ( $kernel, $heap ) = @_[ KERNEL, HEAP ];

  for my $dataset ( keys %{$heap->{MergesPending}} )
    {
      for my $datatype ( keys %{$heap->{MergesPending}->{$dataset}} )
	{
	  for my $version ( keys %{$heap->{MergesPending}->{$dataset}->{$datatype}} )
	    {
	      for my $psethash ( keys %{$heap->{MergesPending}->{$dataset}->{$datatype}->{$version}} )
		{
		  my $latest = 0;
		  foreach my $work ( @{$heap->{MergesPending}->{$dataset}->{$datatype}->{$version}->{$psethash}} )
		    {
		      if ( $work->{received} > $latest )
			{
			  $latest = $work->{received};
			}
		    }

		  if ( (time - $latest) > $heap->{Self}->{AgeThreshold} )
		    {
		      $kernel->yield('merge_submit',($heap->{MergesPending}->{$dataset}->{$datatype}->{$version}->{$psethash}));
		      delete $heap->{MergesPending}->{$dataset}->{$datatype}->{$version}->{$psethash};
		    }
		}
	    }
	}
    }

  $kernel->delay_set('merge_timeout',300);
}

sub merge_submit {
  my ( $self, $kernel, $heap, $worklist ) = @_[ OBJECT, KERNEL, HEAP, ARG0 ];

  if ( scalar @{$worklist} > 1 )
    {
      my $dataset = $worklist->[0]->{Dataset};
      my $datatype =$worklist->[0]->{DataType};
      my $version = $worklist->[0]->{Version};
      my $psethash = $worklist->[0]->{PsetHash};

      my $priority = 99;

      my $id = $heap->{Self}->{Queue}->enqueue($priority,$worklist);

      $self->Quiet("Queue Merge $id for Dataset $dataset, DataType $datatype, Version $version and PSetHash $psethash\n");
    }
  elsif ( scalar @{$worklist} == 1 )
    {
      # pass directly to DBSUpdater
      $worklist->[0]->{DBSUpdate} = 'DBS.RegisterReco';
      delete $worklist->[0]->{RecoReady};
      $self->Log( $worklist->[0] );
    }
}

sub MergeIsPending
{
  my ( $self, $kernel, $heap, $work ) = @_[ OBJECT, KERNEL, HEAP, ARG0 ];

  # record time received
  $work->{received} = time;

  my $dataset = $work->{Dataset};
  my $datatype = $work->{DataType};
  my $version = $work->{Version};
  my $psethash = $work->{PsetHash};

  # check if we should merge this datatype
  my $mergeThis = 0;
  foreach my $allowedDataType ( @{$self->{DataTypes}} )
    {
      if ( $datatype eq $allowedDataType )
	{
	  $mergeThis = 1;
	  last;
	}
    }

  if ( $mergeThis )
    {
      # keep track of this input
      if ( not exists $heap->{MergesPending}->{$dataset}->{$datatype}->{$version}->{$psethash} )
	{
	  my @temp = ();
	  $heap->{MergesPending}->{$dataset}->{$datatype}->{$version}->{$psethash} = \@temp;
	}
      push( @{$heap->{MergesPending}->{$dataset}->{$datatype}->{$version}->{$psethash}}, $work );

      # calculate number of input files, number of events and combined size
      my $count = 0;
      my $events = 0;
      my $size = 0;
      foreach my $temp ( @{$heap->{MergesPending}->{$dataset}->{$datatype}->{$version}->{$psethash}} )
	{
	  $count++;
	  $events += $temp->{NbEvents};
	  $size += $temp->{Sizes};
	}

      # check whether we are above merge threshold
      if ( ( defined $self->{FileThreshold} and $count >= $self->{FileThreshold} ) or
	   ( defined $self->{EventThreshold} and $events >= $self->{EventThreshold} ) or
	   ( defined $self->{SizeThreshold} and $size >= $self->{SizeThreshold} ) )
	{
	  $kernel->yield('merge_submit',($heap->{MergesPending}->{$dataset}->{$datatype}->{$version}->{$psethash}));
	  delete $heap->{MergesPending}->{$dataset}->{$datatype}->{$version}->{$psethash};
	}
    }
  else
    {
      # pass directly to DBSUpdater
      $work->{DBSUpdate} = 'DBS.RegisterReco';
      delete $work->{RecoReady};
      $self->Log( $work );
    }
}

sub AddClient
{
  my $self = shift;
  my $client = shift or Croak "Expected a client name...\n";
  $self->{clients}->{$client} = POE::Queue::Array->new();
}

sub RemoveClient
{
  my $self = shift;
  my $client = shift or Croak "Expected a client name...\n";
  delete $self->{clients}->{$client};
}

sub Queue
{
  my $self = shift;
  my $client = shift;
  return undef unless defined($client);
  if ( ! defined($self->{clients}->{$client}) )
  {
    $self->AddClient($client);
  }
  return $self->{clients}->{$client};
}

sub Clients
{
  my $self = shift;
  my $client = shift;
  if ( defined($client) ) { return $self->{clients}->{$client}; }
  return keys %{$self->{clients}};
}

sub Log
{
  my $self = shift;
  my $logger = $self->{Logger};
  defined $logger && $logger->Send(@_);
}

sub broadcast
{
  my ( $self, $args ) = @_[ OBJECT, ARG0 ];
  my ($work,$priority);
  $work = $args->[0];
  $priority = $args->[1] || 0;

  $self->Quiet("broadcasting... ",$work,"\n");

  foreach ( $self->Clients )
  {
    $self->Quiet("Send: work=\"",$work,"\", priority=",$priority," to $_\n");
    $self->Clients($_)->enqueue($priority,$work);
  }
}

sub file_changed
{
  my ( $self, $kernel, $file ) = @_[ OBJECT, KERNEL, ARG0 ];
  $self->Quiet("Configuration file \"$self->{Config}\" has changed.\n");
  $self->ReadConfig();
  no strict 'refs';
  my $ref = \%{$self->{Partners}->{Worker}};
  my %text = ( 'command' => 'Setup',
               'setup'   => $ref,
             );
  $kernel->yield('broadcast', [ \%text, 0 ] );
}

sub ReadConfig
{
  no strict 'refs';
  my $self = shift;
  my $file = $self->{Config};
  return unless $file;  

  $self->Log("Reading configuration file ",$file);

  my $n = $self->{Name};
  $n =~ s%Manager%Worker%;
  $self->{Partners} = { Worker => $n };
  T0::Util::ReadConfig( $self );

  if ( defined $self->{Watcher} )
  {
    $self->{Watcher}->Interval($self->{ConfigRefresh});
    $self->{Watcher}->Options(\%FileWatcher::Params);
  }

#  if ( $self->{Application} !~ m%^/% )
#  {
#    $self->{Application} = $ENV{T0ROOT} . '/' . $self->{Application};
#  }
}

sub _client_error { reroute_event( (caller(0))[3], @_ ); }
sub client_error
{
  my ( $self, $kernel, $heap ) = @_[ OBJECT, KERNEL, HEAP ];
  my $client = $heap->{client_name};
  $self->Debug($client,": client_error\n");
  $kernel->yield( 'handle_unfinished', $client );
}

sub handle_unfinished
{
  Print "handle_unfinished: Not written yet...\n";
}

sub _client_disconnected { reroute_event( (caller(0))[3], @_ ); }
sub client_disconnected
{
  my ( $self, $kernel, $heap ) = @_[ OBJECT, KERNEL, HEAP ];
  my $client = $heap->{client_name};
  $self->Quiet($client,": client_disconnected\n");
  $kernel->yield( 'handle_unfinished', $client );
}

sub send_setup
{
  my ( $self, $kernel, $heap ) = @_[ OBJECT, KERNEL, HEAP ];
  my $client = $heap->{client_name};

  $self->Quiet("Send: Setup to $client\n");
  no strict 'refs';
  my $ref = \%{$self->{Partners}->{Worker}};
  my %text = ( 'command' => 'Setup',
               'setup'   => $ref,
             );
  $heap->{client}->put( \%text );
}

sub send_start
{
  my ( $self, $kernel, $heap ) = @_[ OBJECT, KERNEL, HEAP ];
  my ($client,%text);
  $client = $heap->{client_name};
  $self->Quiet("Send: Start to $client\n");

  %text = ( 'command' => 'Start',);
  $heap->{client}->put( \%text );
}

sub send_work
{
  my ( $self, $kernel, $heap ) = @_[ OBJECT, KERNEL, HEAP ];
  my ($client,%text,$size,$target);
  my ($priority, $id, $work);

  $client = $heap->{client_name};
  if ( ! defined($client) )
  {
    $self->Quiet("send_work: undefined client!\n");
    return;
  }

  # If there's any client-specific stuff in the queue, send that.
  # Otherwise tell the client to wait
  ($priority, $id, $work) = $self->Queue($client)->dequeue_next(); # if $q;
  if ( $id )
    {
      $self->Verbose("Queued work: ",$work->{command},"\n");
      if ( ref($work) eq 'HASH' )
	{
	  %text = (
		   'client'	=> $client,
		   'priority'	=> $priority,
		   'interval'	=> $self->{Worker}->{Interval},
		  );
	  map { $text{$_} = $work->{$_} } keys %$work;
	  $heap->{client}->put( \%text );
	  return;
	}
      $heap->{idle} = 0;
    }
  else
    {
      ($priority, $id, $work) = $self->{Queue}->dequeue_next();
      if ( ! $id )
	{
	  %text = ( 'command'	=> 'Sleep',
		    'client'	=> $client,
		    'wait'	=> $self->{Backoff} || 10,
		  );
	  $heap->{client}->put( \%text );
	  return;
	}
    }

  # If there was client-specific work, or no work at all, then we don't get
  # here. So I know there is a {File} to report!
  $self->Quiet("Send: Merge job ",$id," to $client\n");
  %text = (
	   'command'	=> 'DoThis',
	   'client'	=> $client,
	   'priority'	=> $priority,
	   'work'	=> $work,
	   'id'         => $id,
	   'svcclass'   => 't0export'
	  );
  $heap->{client}->put( \%text );
}

sub _client_input { reroute_event( (caller(0))[3], @_ ); }
sub client_input
{
  my ( $self, $kernel, $heap, $session, $input ) =
		@_[ OBJECT, KERNEL, HEAP, SESSION, ARG0 ];
  my ( $command, $client );

  $command = $input->{command};
  $client = $input->{client};
  $self->Debug("Got $command from $client\n");

  if ( $command =~ m%HelloFrom% )
  {
    Print "New client: $client\n";
    $heap->{client_name} = $client;
    $self->AddClient($client);
    $kernel->yield( 'send_setup' );
    $kernel->yield( 'send_start' );
    if ( ! --$self->{MaxClients} )
    {
      Print "Telling server to shutdown\n";
      $kernel->post( $self->{Name} => 'shutdown' );
      $self->{Watcher}->RemoveClient($self->{Name});
    }
  }

  if ( $command =~ m%SendWork% )
  {
    $kernel->yield( 'send_work' );
  }

  if ( $command =~ m%JobDone% )
  {
    if ( $input->{status} == 0 )
      {
	$self->Quiet("JobDone: Merge id = $input->{id} succeeded, events = $input->{events}, size = $input->{size}, time = $input->{time}\n");

	# update totals
	$heap->{TotalEvents} += $input->{events};
	$heap->{TotalVolume} += $input->{size};

	my %h = (
		 MonaLisa	 => 1,
		 Cluster	 => $T0::System{Name},
		 Node	 => $self->{Node},
		 TotalEvents => $heap->{TotalEvents},
		 TotalVolume => $heap->{TotalVolume},
		 QueueLength => $self->{Queue}->get_item_count(),
		 NMerge	 => scalar keys %{$self->{clients}},
		);
	$self->Log( \%h );

	my $lfn = $input->{mergefile};
	$lfn =~ s%^/castor/cern.ch/cms/[^/]+%%;
	$lfn =~ s%//%/%g;

	my $guid = $input->{mergefile};
	$guid =~ s%^.*/%%;
	$guid =~ s%\..*$%%;

	# sent notification to DBS updater
	my %g = (
		 DBSUpdate => 'DBS.RegisterReco',
		 Dataset => $input->{Dataset},
		 Version => $input->{Version},
		 PsetHash => $input->{PsetHash},
		 RECOLFNs => $lfn,
		 GUIDs => $guid,
		 CheckSums => $input->{checksum},
		 Sizes => $input->{size},
		 NbEvents => $input->{events},
		);
	$self->Log( \%g );
      }
    else
      {
	$self->Quiet("JobDone: Merge id = $input->{id} failed, status = $input->{status}, reason = $input->{reason}\n");
      }
  }

  if ( $command =~ m%Quit% )
  {
    Print "Quit: $command\n";
    my %text = ( 'command'   => 'Quit',
                 'client' => $client,
               );
    $heap->{client}->put( \%text );
  }
}

1;
