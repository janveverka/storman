use strict;
package T0::Tier0Injector::Manager;
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
$Tier0Injector::Name = 'Tier0Injector::Manager';

our (@queue,%q);

our $hdr = __PACKAGE__ . ':: ';
sub Croak   { croak $hdr,@_; }
sub Carp    { carp  $hdr,@_; }
sub Verbose { T0::Util::Verbose( (shift)->{Verbose}, @_ ); }
sub Debug   { T0::Util::Debug(   (shift)->{Debug},   @_ ); }
sub Quiet   { T0::Util::Quiet(   (shift)->{Quiet},   @_ ); }

sub _init
{
  my $self = shift;

  $self->{Name} = $Tier0Injector::Name;
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
				      send_setup => 'send_setup',
				      send_start => 'send_start',
				      job_done => 'job_done',
				      file_changed => 'file_changed',
				      broadcast	=> 'broadcast',
				     ],
			   ],
       Args => [ $self ],
      );

  $self->{Queue} = POE::Queue::Array->new();
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

  # check if the notification contains all needed information
  #
  # FIXME
  #

  my $priority = 99;

  my $id = $heap->{Self}->{Queue}->enqueue($priority,$work);
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
  $self->{Queue} = POE::Queue::Array->new();
  delete $self->{_queue};
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
  my ($priority, $id, $work) = ( undef, undef, undef );

  $client = $heap->{client_name};
  if ( ! defined($client) )
  {
    $self->Quiet("send_work: undefined client!\n");
    return;
  }

  # If there's any client-specific stuff in the queue, send that.
  # Otherwise tell the client to wait
  ($priority, $id, $work) = $self->Queue($client)->dequeue_next(); # if $q;
  if ( defined $id )
    {
      $self->Quiet("Queued work: ",$work->{command},"\n");
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
      Croak "Why was $work not a hashref for $client?\n";
      $heap->{idle} = 0;
      return;
    }

  if ( $self->{State} eq 'Running' )
    {
      ($priority, $id, $work) = $self->{Queue}->dequeue_next();
      if ( defined $id )
	{
	  $self->Quiet("Send: Tier0Injector job ",$id," to $client\n");
	  %text = (
		   'command'	=> 'DoThis',
		   'client'	=> $client,
		   'priority'	=> $priority,
		   'work'	=> $work,
		   'id'         => $id,
		  );
	  $heap->{client}->put( \%text );

	  return;
	}
    }

  %text = ( 'command'	=> 'Sleep',
	    'client'	=> $client,
	    'wait'	=> $self->{Backoff} || 10,
	  );
  $heap->{client}->put( \%text );
  return;
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
    $kernel->yield('job_done', ($input));
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

sub job_done {
  my ( $self, $kernel, $heap, $session, $input ) = @_[ OBJECT, KERNEL, HEAP, SESSION, ARG0 ];

#  while ( my ($key, $value) = each(%$input) ) {
#        print "Inside JobDone: $key => $value\n";
#    }

  if ( $input->{status} == 0 )
    {
      $self->Quiet("JobDone: Tier0Injector id = $input->{id} succeeded\n");

      my %loghash1 = (
		      TransferStatus => '1',
		      STATUS => 'inserted',
		      FILENAME => basename($input->{work}->{LFN}),
		      STOP_TIME => $input->{work}->{STOP_TIME},
		      T0FirstKnownTime => $input->{work}->{T0FirstKnownTime},
		     );

      if ( exists $input->{work}->{Resent} )
	{
	  $loghash1{Resent} = $input->{work}->{Resent};
	}
      $self->Log( \%loghash1 );
    }
  else
    {
      $self->Quiet("JobDone: Tier0Injector id = $input->{id} failed, status = $input->{status}\n");
    }
}

1;
