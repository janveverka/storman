use strict;
package T0::GenericManager::Manager;
use Sys::Hostname;
use POE;
use POE::Filter::Reference;
use POE::Component::Server::TCP;
use POE::Queue::Array;
use T0::Util;
use T0::FileWatcher;

our (@ISA, @EXPORT, @EXPORT_OK, %EXPORT_TAGS, $VERSION);
my $debug_me=1;

use Carp;
$VERSION = 1.00;
@ISA = qw/ Exporter /;
$GenericManager::Name = 'GenericManager::Manager';

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

  $self->{Name} = $GenericManager::Name;
  my %h = @_;
  map { $self->{$_} = $h{$_}; } keys %h;
  $self->ReadConfig();
  check_host( $self->{Host} ); 

  foreach ( qw / RecoTimeout / )
  {
    $self->{$_} = 0 unless defined $self->{$_};
  }
  Croak "undefined Application\n" unless defined $self->{Application};

  POE::Component::Server::TCP->new
  ( Port                => $self->{Port},
    Alias               => $self->{Name},
    ClientFilter        => "POE::Filter::Reference",
    ClientInput         => \&_client_input,
    ClientDisconnected  => \&_client_disconnected,
    ClientError         => \&_client_error,
    Started             => \&_started,
    ObjectStates	=> [
	$self => [
		        started	=> 'started',
		   client_input	=> 'client_input',
		   client_error	=> 'client_error',
	    client_disconnected	=> 'client_disconnected',
      	      handle_unfinished => 'handle_unfinished',
		      send_work => 'send_work',
		     send_setup => 'send_setup',
		     send_start => 'send_start',
		   file_changed => 'file_changed',
		      broadcast	=> 'broadcast',
		   InputPending => 'InputPending',
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

our @attrs = ( qw/ Name Host Port / );
our %ok_field;
for my $attr ( @attrs ) { $ok_field{$attr}++; }

sub AUTOLOAD {
  my $self = shift;
  my $attr = our $AUTOLOAD;
  $attr =~ s/.*:://;
  return unless $attr =~ /[^A-Z]/;  # skip DESTROY and all-cap methods
  Croak "AUTOLOAD: Invalid attribute method: ->$attr()" unless $ok_field{$attr};
  if ( @_ ) { Croak "Setting attributes not yet supported!\n"; }
# $self->{$attr} = shift if @_;
  return $self->{$attr};
}

sub InputPending
{
  my ( $self, $kernel, $heap, $work ) = @_[ OBJECT, KERNEL, HEAP, ARG0 ];

  my ($priority, $id);
  $priority = 99;
  $work->{work} = $self->{Application};
  $id = $self->{Queue}->enqueue($priority,$work);
  $self->Quiet("Task $id is queued for ",$work->{File},"\n");
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
  if ( defined($client) ) { return $self->{clients}->{$client}; }
  return undef;
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

sub _started
{
  my ( $self, $kernel, $session ) = @_[ ARG0, KERNEL, SESSION ];
  my %param;

  $self->Debug($self->{Name}," has started...\n");
  $self->Log($self->{Name}," has started...\n");
  $self->{Session} = $session->ID;

  $kernel->state( 'send_setup',   $self );
  $kernel->state( 'file_changed', $self );
  $kernel->state( 'broadcast',    $self );
  $kernel->state( 'InputPending', $self );

  %param = ( File     => $self->{Config},
             Interval => $self->{ConfigRefresh},
             Client   => $self->{Name},
             Event    => 'file_changed',
           );
  $self->{Watcher} = T0::FileWatcher->new( %param );
  $kernel->yield( 'file_changed' );
}

sub started
{
  Croak "Great, what am I doing here...?\n";
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
    $self->Quiet("Send work=\"",$work,"\", priority=",$priority," to $_\n");
    $self->Clients($_)->enqueue($priority,$work);
  }
}

sub file_changed
{
  my ( $self, $kernel, $file ) = @_[ OBJECT, KERNEL, ARG0 ];
  $self->Quiet("Configuration file \"$self->{Config}\" has changed.\n");
  $self->ReadConfig();
}

sub ReadConfig
{
  no strict 'refs';
  my $self = shift;
  my $file = $self->{Config};
  return unless $file;  

  $self->Log("Reading configuration file ",$file);

  $self->{Partners} = { Worker => 'GenericManager::Worker' };
  T0::Util::ReadConfig( $self );

  if ( defined $self->{Watcher} )
  {
    $self->{Watcher}->Interval($self->{ConfigRefresh});
    $self->{Watcher}->Options(\%FileWatcher::Params);
  }

  if ( $self->{Application} !~ m%^/% )
  {
    $self->{Application} = $ENV{T0ROOT} . '/' . $self->{Application};
  }
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
  Carp "handle_unfinished: Not written yet...\n";
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
  my %text = ( 'command' => 'Setup',
               'setup'   => \%GenericManager::Worker,
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
  my ($q, $priority, $id, $work);

  $client = $heap->{client_name};

# If there's any client-specific stuff in the queue, send that. Otherwise,
# tell the client to wait
  $q = $self->Queue($client);
  ($priority, $id, $work) = $q->dequeue_next();
  if ( $id )
  {
    $self->Verbose("Queued work: $work\n");
    if ( ref($work) eq 'HASH' )
    {
      %text = ( 'client'	=> $client,
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
		'wait'		=> $self->{Backoff} || 10,
	      );
      $heap->{client}->put( \%text );
      return;
    }
  }
  $self->Quiet("Send: work=$work, priority=$priority to $client\n");
  $work->{id} = $id;
  %text = ( 'command'	=> 'DoThis',
            'client'	=> $client,
	    'work'	=> $work,
	    'priority'	=> $priority,
	  );
  $heap->{client}->put( \%text );
  $kernel->yield( 'SetRecoTimer', $id );
}

sub _client_input { reroute_event( (caller(0))[3], @_ ); }
sub client_input
{
  my ( $self, $kernel, $heap, $session, $input ) =
		@_[ OBJECT, KERNEL, HEAP, SESSION, ARG0 ];
  my ( $command, $client );

  $command = $input->{command};
  $client = $input->{client};

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
    my $work     = $input->{work};
    my $status   = $input->{status};
    my $priority = $input->{work}{priority};
    my $id       = $input->{id};
    $self->Quiet("JobDone: work=$work, priority=$priority, id=$id, status=$status\n");

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
