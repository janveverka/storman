use strict;
package T0::Copy::Manager;
use Sys::Hostname;
use POE;
use POE::Filter::Reference;
use POE::Component::Server::TCP;
use POE::Queue::Array;
use T0::Util;
use T0::FileWatcher;
use File::Basename;

our (@ISA, @EXPORT, @EXPORT_OK, %EXPORT_TAGS, $VERSION);

use Carp;
$VERSION = 1.00;
@ISA = qw/ Exporter /;
$Copy::Name = 'Copy::Manager';

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

  $self->{Name} = $Copy::Name;
  my %h = @_;
  map { $self->{$_} = $h{$_}; } keys %h;
  $self->ReadConfig();
  check_host( $self->{Host} ); 

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
				      send_client_work => 'send_client_work',
				      send_setup => 'send_setup',
				      send_start => 'send_start',
				      job_done => 'job_done',
				      file_changed => 'file_changed',
				      broadcast	=> 'broadcast',
				     ],
			   ],
       ListenerArgs => [ $self ],
      );

  $self->{State} = 'Running';
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
  $self->{TotalEvents} = 0;
  $self->{TotalVolume} = 0;

  $self->Debug($self->{Name}," has started...\n");
  $self->Log($self->{Name}," has started...\n");
  $self->{Session} = $session->ID;

  $kernel->state( 'send_setup', $self );
  $kernel->state( 'file_changed', $self );
  $kernel->state( 'broadcast', $self );
  $kernel->state( 'SetState', $self );

  $kernel->state( 'job_done', $self );

  $kernel->state( 'process_file', $self );

  %param = ( File     => $self->{Config},
             Interval => $self->{ConfigRefresh},
             Client   => $self->{Name},
             Event    => 'file_changed',
           );
  $self->{Watcher} = T0::FileWatcher->new( %param );
}

sub process_file
{
  my ( $self, $kernel, $heap, $work ) = @_[ OBJECT, KERNEL, HEAP, ARG0 ];

  # record time received
  $work->{received} = time;

  if ( not defined $work->{SvcClass} )
    {
      $work->{SvcClass} = 't0input';
    }

  # check if the job is for a black listed run
  if ( exists $self->{RunBlacklist} and defined $self->{RunBlacklist} )
    {
      foreach my $run (@{$self->{RunBlacklist}})
	{
	  if ( $run == int($work->{RUNNUMBER}) )
	    {
	      $self->Quiet("Received job for blacklisted run " . $run . ", discarding it\n");
	      return;
	    }
	}
    }

  # checksum format workaround
  if ( defined($work->{CHECKSUM}) ) {
      if ( $work->{CHECKSUM} =~ m/0*([a-f0-9]+)(.*)/ ) {
	  if ( length($2) == 0 and length($1) <= 8 ) {
	      $work->{CHECKSUM} = $1;
	  }
	  else {
	      $work->{CHECKSUM} = undef;
	  }
      }
      else{
	  $work->{CHECKSUM} = undef;
      }
  }

  my $priority = 99;
  if( $work->{STREAM} ) {
    if( exists $self->{HighPrioStream} && $work->{STREAM} =~ /$self->{HighPrioStream}/ ) {
      $priority = 33;
    }
    elsif( exists $self->{NormPrioStream} && $work->{STREAM} =~ /$self->{NormPrioStream}/ ) {
      $priority = 66;
    }
  }

  my $hostname = delete $work->{HOSTNAME};

  my $id = $self->HostnameQueue($hostname)->enqueue($priority,$work);;
  $self->Quiet("Job $id added to ", $hostname, " queue\n");
}

# Create 2 private queues (for the hostname and client)
# Arg1: $client
sub AddClient
{
  my $self = shift;
  my $client = shift or Croak "Expected a client name...\n";
  my $hostname = $self->{hostnames}->{$client};

  $self->{clientsQueue}->{$client} = POE::Queue::Array->new();

  $self->AddHostname($hostname);

  # Add the client to the list of clients serving this hostname, and cache its ID
  my $clientid = push @{$self->{clientList}->{$hostname}}, $client;
  $self->{clientIDs}->{$client} = $clientid;
}

# Create hostname's queue
# Arg1: $hostname
sub AddHostname
{
  my $self = shift;
  my $hostname = shift or Croak "Expected a hostname...\n";

  if( !defined( $self->{hostnamesQueue}->{$hostname} ) )
    {
      $self->{hostnamesQueue}->{$hostname} = POE::Queue::Array->new();
    }
}

# Remove the hostname and client private queues
# Arg1: $client
sub RemoveClient
{
  my $self = shift;
  my $client = shift or do {
    # No client received, so most certainly nothing to clean
    # No point in dying there, just log and continue
    # Otherwise it ight crash when a CopyWorker's machine crashes
    Carp "Expected a client - Cannot remove nothing!\n";
    return;
  };
  my $hostname = delete $self->{hostnames}->{$client};

  $self->Verbose("Removing $client on $hostname\n");
  delete $self->{clientsQueue}->{$client};
  delete $self->{clientIDs}->{$client};

  # Remove the hostname queue only if there are no more clients using it.
  my $remainingWorkers = scalar grep { $_ eq $hostname }
    keys(%{$self->{hostnames}});

  if( ! $remainingWorkers )
    {
      delete $self->{clientList}->{$hostname}; # Delete ID list

      # Check hostnameQueue content, and delete it if it's empty
      if( $self->{hostnamesQueue}->{$hostname} && my $count = $self->{hostnamesQueue}->{$hostname}->get_item_count() )
        {
          $self->Quiet("Queue for $hostname contains $count items: kept.\n");
        }
        else {
          delete $self->{hostnamesQueue}->{$hostname};
        }
    }
  else {

    # One client disconnected, update the list of IDs
    my $clientList = $self->{clientList}->{$hostname};
    $clientList = [ grep { ! /^$client$/xms } @{$clientList} ];
    for my $clientIndex ( 0 ..  $#{$clientList} ) {
      my $client = $clientList->[$clientIndex];
      my $clientID = $clientIndex + 1;
      $self->{clientIDs} = $clientID;
      my $id = $self->ClientQueue($client)->enqueue(1, {
        command => 'SetID', clientID => $clientID }, );
      $self->Quiet("SetID = $clientID job $id added to $client queue\n");
    }
  }
}

# Return the hostname's private queue
# Arg1: $hostname
sub HostnameQueue
{
  my $self = shift;
  my $hostname = shift;

  return undef unless defined($hostname);
  if( !defined($self->{hostnamesQueue}->{$hostname}) )
    {
      $self->AddHostname($hostname);
    }
  return $self->{hostnamesQueue}->{$hostname};
}

# Return the client's private queue
# Arg1: $client
sub ClientQueue
{
  my $self = shift;
  my $client = shift;

  return undef unless defined($client);
  if( !defined($self->{clientsQueue}->{$client}) )
    {
      $self->AddClient($client);
    }
  return $self->{clientsQueue}->{$client};
}

sub Log
{
  my $self = shift;
  my $logger = $self->{Logger};
  defined $logger && $logger->Send(@_);
}

# Send a work to every client (to their private queues)
# Arg1: [$work,$priority]
sub broadcast
{
  my ( $self, $args ) = @_[ OBJECT, ARG0 ];
  my ($work,$priority);
  $work = $args->[0];
  $priority = $args->[1] || 0;

  $self->Quiet("Broadcasting... ",$work,"\n");
  foreach ( keys %{$self->{clientsQueue}} )
  {
    my $id = $self->ClientQueue($_)->enqueue($priority,$work);
    $self->Quiet("Broadcast job $id added to $_ queue\n");
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
    if ( keys(%FileWatcher::Params) > 0 )
    {
	$self->{Watcher}->Options(\%FileWatcher::Params);
    }
  }
}

sub SetState
{
  my ( $self, $kernel, $heap, $input ) = @_[ OBJECT, KERNEL, HEAP, ARG0 ];
  $self->Quiet("State control: ",T0::Util::strhash($input),"\n");
  return if $self->{State} eq $input->{SetState};
  $kernel->yield( 'FSM_' . $input->{SetState}, $input );
}

sub FSM_Abort
{
  my ( $self, $kernel, $heap ) = @_[ OBJECT, KERNEL, HEAP ];
  Print "I am in FSM_Abort. Empty the queue and forget all allocated work.\n";
  # TODO: empty all hostname queues
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
  my ( $self, $kernel, $heap, $client ) = @_[ OBJECT, KERNEL, HEAP, ARG0 ];
  Print "handle_unfinished: Not implemented yet...\n";
}

sub _client_disconnected { reroute_event( (caller(0))[3], @_ ); }
sub client_disconnected
{
  my ( $self, $kernel, $heap ) = @_[ OBJECT, KERNEL, HEAP ];
  my $client = $heap->{client_name};
  $self->Quiet($client,": client_disconnected\n");
  $kernel->yield( 'handle_unfinished', $client );
  $self->RemoveClient( $client );
}

sub send_setup
{
  my ( $self, $kernel, $heap ) = @_[ OBJECT, KERNEL, HEAP ];
  my $client = $heap->{client_name};

  $self->Quiet("Send: Setup to $client\n");
  $heap->{client}->put( { command => 'SetID', clientID =>
    $self->{clientIDs}->{$client}, } );
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

sub send_client_work {
  my ( $self, $kernel, $heap ) = @_[ OBJECT, KERNEL, HEAP ];
  my ($client,$hostname,%text,$size,$target);

  $client = $heap->{client_name};
  $hostname = $heap->{hostname};

  if ( !defined($client) || !defined($hostname) )
  {
    $self->Quiet("send_client_work: undefined client or hostname!\n");
    return;
  }

  # Check client's queue
  my ($priority, $id, $work) =
    $self->ClientQueue($client)->dequeue_next();
  if ( defined $id ) {
      $self->Debug("Job $id taken from client's queue.\n");
      $self->Quiet("Send: New config job $id to $client\n");

      if ( ref($work) eq 'HASH' ) {
	    %text = (
		   'client'	=> $client,
		   'priority'	=> $priority,
		   'interval'	=> $self->{Worker}->{Interval},
		  );
	    map { $text{$_} = $work->{$_} } keys %$work;
	    $heap->{client}->put( \%text );
	  }
      else {
	    Croak "Why was $work not a hashref for $client?\n";
	    $heap->{idle} = 0;
	  }
  }
  return $id;
}

# Check hostname's queue
sub send_work
{
  my ( $self, $kernel, $heap ) = @_[ OBJECT, KERNEL, HEAP ];
  my ($client,$hostname,%text,$size,$target);
  my ($priority, $id, $work);

  $client = $heap->{client_name};
  $hostname = $heap->{hostname};

  if ( !defined($client) || !defined($hostname) )
  {
    $self->Quiet("send_work: undefined client or hostname!\n");
    return;
  }

  if ( $self->{State} eq 'Running' )
    {
      # loop over job and discard the ones for blacklisted runs
      while ( ($priority, $id, $work) = $self->HostnameQueue($hostname)->dequeue_next() )
	{
	  if ( defined $id and defined $work )
	    {
	      $self->Debug("Job $id (prio: $priority) for run $work->{RUNNUMBER} taken from $hostname queue.\n");

	      if ( exists $self->{RunBlacklist} and defined $self->{RunBlacklist} )
		{
		  my %runBlacklistHash = (@{$self->{RunBlacklist}},@{$self->{RunBlacklist}});

		  if ( exists $runBlacklistHash{int($work->{RUNNUMBER})} )
		    {
		      $self->Debug("Dequeued job for blacklisted run " . $work->{RUNNUMBER} . ", discarding it\n");
		      next;
		    }
		}

	      # leave loop, have valid work unit or there is no work unit
	      last;
	    }
	}
    }

  if ( defined $id and defined $work )
    {
      # send copy job to client
      $self->Quiet("Send: Copy job ",$id," to $client\n");
      %text = (
	       'command'  => 'DoThis',
	       'client'	  => $client,
	       'priority' => $priority,
	       'work'	  => $work,
	       'id'       => $id,
	      );
      $heap->{client}->put( \%text );
    }
  else
    {
      # put client to sleep
      my $wait = $self->{Backoff} || 10;
      $self->Quiet("Send: Sleep $wait to $client\n");
      %text = ( 'command' => 'Sleep',
		'client'  => $client,
		'wait'	  => $wait,
	      );
      $heap->{client}->put( \%text );
    }
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

    # Store clientname and hostname in the heap
    $heap->{client_name} = $client;
    $heap->{hostname} = $input->{hostname};

    # Store relation hostname-clientname in the object
    $self->{hostnames}->{$heap->{client_name}} = $heap->{hostname};

    $self->AddClient($heap->{client_name});
    $kernel->yield( 'send_setup' );
    $kernel->yield( 'send_start' );
    if ( ! --$self->{MaxClients} )
    {
      Print "Telling server to shutdown\n";
      $kernel->post( $self->{Name} => 'shutdown' );
      $self->{Watcher}->RemoveClient($self->{Name});
    }
  }

  elsif ( $command =~ m%SendWork% )
  {
    if( ! $kernel->call( $session, 'send_client_work' ) ) {
      $kernel->yield( 'send_work' );
    }
  }

  elsif ( $command =~ m%SendClientWork% )
  {
    if( ! $kernel->call( $session, 'send_client_work' ) )
    {
      # put client to sleep
      $self->Quiet("Send: Sleep 300 to $client\n");
      my %text = ( 'command' => 'Sleep',
		client  => $client,
		wait    => 300,
	      );
      $heap->{client}->put( \%text );
    }
  }

  elsif ( $command =~ m%JobDone% )
  {
    $kernel->yield('job_done', ($input));
  }

  elsif ( $command =~ m%Quit% )
  {
    Print "Quit: $command\n";
    my %text = ( 'command'   => 'Quit',
                 'client' => $client,
               );
    $heap->{client}->put( \%text );
  }
  elsif ( $command =~ m/GetID/ ) {
    my %text = ( command => 'SetID',
                 clientID => $self->{clientIDs}->{$client},
               );
    $heap->{client}->put( \%text );
  }
}

sub job_done {
  my ( $self, $kernel, $heap, $session, $input ) = @_[ OBJECT, KERNEL, HEAP, SESSION, ARG0 ];

#  while ( my ($key, $value) = each(%$input) ) {
#        print "Inside JobDone: $key => $value\n";
#    }

  if ( $input->{status} == 0 )
    {
      $self->Quiet("JobDone: Copy id = $input->{id} succeeded\n");

      if ( $input->{work}->{PFN} ne '/dev/null' )
	{
	  my %loghash1 = (
			  TransferStatus => '1',
			  STATUS => 'copied',
			  FILENAME => basename($input->{work}->{PFN}),
			  STOP_TIME => $input->{work}->{STOP_TIME},
			  T0FirstKnownTime => $input->{work}->{T0FirstKnownTime},
			 );
	  if ( exists $input->{work}->{Resent} )
	    {
	      $loghash1{Resent} = $input->{work}->{Resent};
	    }

	  $self->Log( \%loghash1 );

	  my %loghash2 = (
			  OnlineFile => 't0input.available',
			  RUNNUMBER => $input->{work}->{RUNNUMBER},
			  LUMISECTION => $input->{work}->{LUMISECTION},
			  PFN => $input->{work}->{PFN},
			  NEVENTS => $input->{work}->{NEVENTS},
			  START_TIME => $input->{work}->{START_TIME},
			  STOP_TIME => $input->{work}->{STOP_TIME},
			  SETUPLABEL => $input->{work}->{SETUPLABEL},
			  STREAM => $input->{work}->{STREAM},
			  FILESIZE => $input->{work}->{FILESIZE},
			  CHECKSUM => $input->{work}->{CHECKSUM},
			  TYPE => $input->{work}->{TYPE},
			  APP_NAME => $input->{work}->{APP_NAME},
			  APP_VERSION => $input->{work}->{APP_VERSION},
			  HLTKEY => $input->{work}->{HLTKEY},
			  DeleteAfterCheck => $input->{work}->{DeleteAfterCheck},
			  SvcClass => $input->{work}->{SvcClass},
			  T0FirstKnownTime => $input->{work}->{T0FirstKnownTime},
			  InjectIntoTier0 => $input->{work}->{InjectIntoTier0},
			 );

	  if ( exists $input->{work}->{LFN} )
	    {
	      $loghash2{LFN} = $input->{work}->{LFN}
	    }

	  if ( exists $input->{work}->{Resent} )
	    {
	      $loghash2{Resent} = $input->{work}->{Resent};
	    }

	  $self->Log( \%loghash2 );
	}
    }
  else
    {
      $self->Quiet("JobDone: Copy id = $input->{id} failed, status = $input->{status}\n");
    }
}

1;
