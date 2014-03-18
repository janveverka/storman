use strict;
package T0::Component::Manager;
use File::Basename;
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
$ComponentManager::Name = 'Component::Manager';

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

  $self->{Name} = $ComponentManager::Name;
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
		     check_rate	=> 'check_rate',
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

sub FSM_Abort
{
  my ( $self, $kernel, $heap ) = @_[ OBJECT, KERNEL, HEAP ];
  Print "I am in FSM_Abort. Empty the queue and forget all allocated work.\n";
  $self->{Queue} = POE::Queue::Array->new();
  delete $self->{_queue};
}

sub FSM_Flush
{
  my ( $self, $kernel, $heap ) = @_[ OBJECT, KERNEL, HEAP ];
  Print "I am in FSM_Flush. I only empty the queue.\n";
  $self->{Queue} = POE::Queue::Array->new();
}

sub FSM_Pause
{
  my ( $self, $kernel, $heap ) = @_[ OBJECT, KERNEL, HEAP ];
  Print "I am in FSM_Pause\n";
  $self->{State} = 'Pause';
}

sub FSM_Quit
{
  my ( $self, $kernel, $heap ) = @_[ OBJECT, KERNEL, HEAP ];
  Print "I am in FSM_Quit. I will shoot myself. Arrgh!\n";
  exit 0;
}

sub FSM_Resume
{
  my ( $self, $kernel, $heap ) = @_[ OBJECT, KERNEL, HEAP ];
  Print "I am in FSM_Resume\n";
  $self->{State} = 'Running';
}

sub RecoIsPending
{
  my ( $self, $kernel, $heap, $work ) = @_[ OBJECT, KERNEL, HEAP, ARG0 ];

  my ($f, $priority, $id);
  $priority = 99;
  $f = $work->{File};
  foreach ( keys %{$self->{Priorities}} )
  {
    if ( $f =~ m%$_% )
    {
      $priority = $self->{Priorities}{$_};
    }
  }
  $work->{work} = $self->{Application};

  $id = $self->{Queue}->enqueue($priority,$work);
  $self->Quiet("Reco $id is queued for ",$work->{File},"\n");
}

sub SetRecoTimer
{
  my ( $self, $kernel, $heap, $id ) = @_[ OBJECT, KERNEL, HEAP, ARG0 ];
  return unless $id;
  $self->{_queue}{$id}{Start} = time;
  return unless $self->{RecoTimeout};
  my $delay = $kernel->delay_set('RecoIsStale',$self->{RecoTimeout},$id);
  $self->Verbose("SetRecoTimer: Delay ID: $delay Reco ID: $id\n");
  $self->{Reco}{DelayID}{$id} = $delay;
}

sub RecoIsStale
{
  my ( $self, $kernel, $heap, $id ) = @_[ OBJECT, KERNEL, HEAP, ARG0 ];
  my ($x,$lid,$did);
  $self->Verbose("Check if Reco $id is stale...\n");
  return unless defined($self->{_queue}{$id});
  $x = $self->{_queue}{$id};
  my $age = time - $x->{Start};
  $self->Quiet("Reco $id is stale (age: $age seconds)\n");
  $self->CleanupReco($id);
}

sub CleanupReco
{
  my ($self,$id) = @_;
  my ($x,$lid,$did);

  $self->Quiet("RecoID $id is being deleted!\n");

  $x = $self->{_queue}{$id};
  $did = $x->{Reco};
  foreach $lid ( @{$x->{Reco}} )
  {
    if ( ! defined($lid) || ! defined($self->{lumi}{$lid}) )
    {
      $DB::single=$debug_me;
    }
    my $count = $self->{lumi}{$lid}{Reco}{$did};
    $self->Quiet("Reco $id: Lumi $lid: Reco $did: Count $count\n");
    delete $self->{lumi}{$lid}{Reco}{$did};
    my $i = scalar keys %{$self->{lumi}{$lid}{Reco}};
    if ( $i )
    {
      $self->Quiet("Reco $id: Lumi $lid: Reco left: $i\n");
    }
    else
    {
      $self->Quiet("LumiID $lid is complete, delete it!\n");
      $self->DeleteReco($lid);
    }
  }
  delete $self->{_queue}{$id};
}

sub RecoHasBeenProcessed
{
  my ( $self, $kernel, $heap, $did ) = @_[ OBJECT, KERNEL, HEAP, ARG0 ];
Print "RecoHasBeenProcessed: Not yet written...\n";
  my ($type,$file);
  $self->CleanupReco($did);
}

sub AddClient
{
  my $self = shift;
  my $client = shift or Croak "Expected a client name...\n";
  $self->{queues}->{$client} = POE::Queue::Array->new();
  $_ = shift;
  if ( $_ ) { $self->{clients}{$client} = $$_; }
}

sub RemoveClient
{
  my $self = shift;
  my $client = shift or Croak "Expected a client name...\n";
  delete $self->{queues}->{$client};
}

sub Queue
{
  my $self = shift;
  my $client = shift;
  return undef unless defined($client);
  if ( ! defined($self->{queues}->{$client}) )
  {
    $self->AddClient($client);
  }
  return $self->{queues}->{$client};
}

sub Clients
{
  my $self = shift;
  my $client = shift;
  if ( defined($client) ) { return $self->{queues}->{$client}; }
  return keys %{$self->{queues}};
}

my %Stats;
sub check_rate
{
  my ( $self, $kernel, $heap, $session ) = @_[  OBJECT, KERNEL, HEAP, SESSION ];
  $self->{StatisticsInterval} = 60 unless defined($self->{StatisticsInterval});
  $kernel->delay_set( 'check_rate', $self->{StatisticsInterval} );

  my ($i,$size,$nev,$s,%g,$channel);
$DB::single=$debug_me;
  $s = $self->{StatisticsInterval};
  while ( $_ = shift @{$self->{stats}} )
  {
    $channel = $_->{channel};
    $g{$channel}{size} += $_->{size};
    $g{$channel}{nev}  += $_->{nev};
    $g{$channel}{readings}++;;
  }

  $size = $nev = 0;
  foreach $channel ( sort keys %g )
  {
    my %h;
    $size = int($g{$channel}{size}*100/1024/1024)/100;
    $nev  = $g{$channel}{nev};
    $i    = $g{$channel}{readings};

    $Stats{TotalEvents} += $nev;
    $Stats{TotalVolume} += $size;

    Print "Channel = $channel, Events = $nev, Volume = $size\n";
    $self->Debug("$size MB, $nev events in $s seconds, $i readings\n");
    %h = (   MonaLisa	 => 1,
	     Cluster	 => $T0::System{Name},
             Node	 => $self->{Node} . '_' . $channel,
             Events	 => $nev,
	     RecoVolume  => $size,
             Readings	 => $i,
         );
    $self->Log( \%h );
  }
  Print "TotalEvents = $Stats{TotalEvents}, TotalVolume = $Stats{TotalVolume}\n";
  my %h = (  MonaLisa	 => 1,
	     Cluster	 => $T0::System{Name},
             Node	 => $self->{Node},
	     TotalEvents => $Stats{TotalEvents},
	     TotalVolume => $Stats{TotalVolume},
	     QueueLength => $self->{Queue}->get_item_count(),
	     NReco	 => scalar keys %{$self->{queues}},
	     NActive	 => scalar keys %{$self->{_queue}},
           );
  $self->Log( \%h );
}

sub GatherStatistics
{
  my ($self,$input) = @_;
  my ($nev);

# Use $input->{Channel} to separate the counts...
$DB::single=$debug_me;

#  foreach ( @{$input->{stderr}} )
#  {
#    if ( m%Run:\s+(\d+)\s+Event:\s+(\d+)% ) { $h{run} = $1; $h{nev} = $2; }
#  }
#  if ( defined($h{nev}) && defined($input->{NbEvents}) )
#  {
#    if ( $h{nev} != $input->{NbEvents} )
#    {
#      Print "nev != NbEvents: ",$h{nev},' ',$input->{NbEvents},"\n";
#    }
#    $h{nev} = $input->{NbEvents};
#  }

  if ( exists($input->{Files}) )
  {
#   If I get an array, handle each stream separately
    foreach ( keys %{$input->{Files}} )
    {
      my %g;
      $g{channel} = $input->{Files}->{$_}->{Module};
      $g{size}	  = $input->{Files}->{$_}->{Size};
      $g{nev}	  = $input->{Files}->{$_}->{NbEvents};
      push @{$self->{stats}}, \%g;
    }
  }
  else
  {
    my %h;
    $h{channel}	= $input->{Channel};
    $h{nev}	= $input->{NbEvents};
    $h{size}	= $input->{Sizes};
# return unless ( defined($h{nev}) && defined($h{size}) );
    return unless ( defined($h{nev}) );
    push @{$self->{stats}}, \%h;
  }
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
  $self->Log($self->{Name}." has started...\n");
  $self->{Session} = $session->ID;

  $kernel->state( 'send_setup',   $self );
  $kernel->state( 'file_changed', $self );
  $kernel->state( 'broadcast',    $self );
  $kernel->state( 'SetState',     $self );
  $kernel->state( 'SetRecoTimer',		$self );
  $kernel->state( 'RecoIsStale',		$self );
  $kernel->state( 'RecoIsPending',		$self );
  $kernel->state( 'RecoHasBeenProcessed',	$self );

  $kernel->state( 'FSM_Abort',	$self );
  $kernel->state( 'FSM_Flush',	$self );
  $kernel->state( 'FSM_Pause',	$self );
  $kernel->state( 'FSM_Quit',	$self );
  $kernel->state( 'FSM_Resume',	$self );

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

  $self->Quiet("broadcasting... ",T0::Util::strhash($work),"\n");

  foreach ( values %{$self->{clients}} )
  {
    $self->Quiet("Send: ",T0::Util::strhash($work)," to $_\n");
    $_->put($work);
#   $self->Clients($_)->enqueue($priority,$work);
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

  $self->Log($self->{Name}.": Reading configuration file: ".$file);

  my $n = $self->{Name};
  $n =~ s%Manager%Worker%;
  $self->{Partners} = { Worker => $n };
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

sub SetState
{
  my ( $self, $kernel, $heap, $input ) = @_[ OBJECT, KERNEL, HEAP, ARG0 ];
  $self->Quiet("State control: ",T0::Util::strhash($input),"\n");
  return if $self->{State} eq $input->{SetState};
  $kernel->yield( 'FSM_' . $input->{SetState}, $input );

# This is also a but ugly... :-(
  $input->{command} = 'SetState';
  $kernel->yield('broadcast', [ $input, 0 ] );
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
  Print "handle_unfinished: Not written yet. Should call RemoveClient and recycle unfinished queue entries.\n";
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

  %text = ( command	=> 'SetState',
	    SetState	=> 'Start', );
  $heap->{client}->put( \%text );
}

sub send_work
{
  my ( $self, $kernel, $heap ) = @_[ OBJECT, KERNEL, HEAP ];
  my ($client,%text,$size,$target);
  my ($q, $priority, $id, $work);

  $client = $heap->{client_name};
  if ( ! defined($client) )
  {
    $self->Quiet("send_work: undefined client!\n");
    return;
  }

# If there's any client-specific stuff in the queue, send that first.
# If the state is running, and there's non-client-specific stuff, send that
# Otherwise, tell the client to wait

  $q = $self->Queue($client);
  ($priority, $id, $work) = $q->dequeue_next(); # if $q;
  if ( $id )
  {
    $self->Verbose("Queued work: ",$work->{command},"\n");
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
    Croak "Why was $work not a hashref for $client?\n";
    $heap->{idle} = 0;
    return;
  }

  undef $id; # Redundant, but safe...
  if ( $self->{State} eq 'Running' )
  {
    ($priority, $id, $work) = $self->{Queue}->dequeue_next();
  }
  if ( $id ) # True only if we are 'Running' and there was queued work
  {
    $self->Quiet("Send: ",T0::Util::strhash($work)," to $client\n");
    $work->{id} = $id;
    %text = ( 'command'	 => 'DoThis',
              'client'	 => $client,
	      'work'	 => $work,
	      'priority' => $priority,
	    );
    $heap->{client}->put( \%text );

#   Why do I need to do this here...?
    $self->{_queue}{$id}{Start} = time;

    $kernel->yield( 'SetRecoTimer', $id );
    return;
  }

  %text = ( 'command'	=> 'Sleep',
            'client'	=> $client,
	    'wait'	=> $self->{Backoff} || 60,
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
  $self->Debug("Got ",T0::Util::strhash($input)," from $client\n");

  if ( $command =~ m%HelloFrom% )
  {
    Print "New client: $client\n";
    $heap->{client_name} = $client;
    $self->AddClient($client,\$heap->{client});
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
    $kernel->yield( 'send_work' );
  }

  elsif ( $command =~ m%JobDone% )
  {
    my $work     = $input->{work};
    my $status   = $input->{status};
#   my $priority = $input->{work}{priority};
    my $id       = $input->{Parent}{id};
    $self->Quiet("JobDone: work=",T0::Util::strhash($work),", id=$id, status=$status\n");

#   Check rate statistics from the first client onwards...
    if ( !$self->{client_count}++ ) { $kernel->yield( 'check_rate' ); }

#    if ( $input->{RecoFile} )
#    {
#      my %h = (	MonaLisa	=> 1,
#		Cluster		=> $T0::System{Name},
#		Node		=> $self->{Node},
#		QueueLength	=> $self->{Queue}->get_item_count(),
#		NReco		=> scalar keys %{$self->{queues}},
#		NActive		=> scalar keys %{$self->{_queue}},
#	      );
#      if ( exists($self->{_queue}{$id}{Start}) )
#      {
#        $h{Duration} = time - $self->{_queue}{$id}{Start};
#      }
#      $self->Log( \%h );
#$DB::single=$debug_me;
#      my $guid = $input->{RecoFile};
#      $guid =~ s%^.*/%%;
#      $guid =~ s%\..*$%%;
#      my $lfn = $input->{RecoFile};
#      $lfn =~ s%^/castor/cern.ch/cms%%;
#      $lfn =~ s%//%/%g;
#      my %g = ( RecoReady  => 'DBS.RegisterReco',
#		T0Name	   => $T0::System{Name},
#		SizesA	   => $input->{RecoSize},
#		SizesB	   => int($input->{RecoSize} * 1024 * 1024 + 0.4),
#		PsetHash   => $input->{PsetHash},
#		Version    => $input->{Version},
#		CheckSums  => $input->{Files}->{basename $input->{RecoFile}}->{Checksum},
#		Sizes      => $input->{Files}->{basename $input->{RecoFile}}->{Size},
#		GUIDs	   => $guid,
#		Dataset	   => $input->{Channel} . $input->{DatasetNumber},
#		NbEvents   => $input->{NbEvents},
#		RECOLFNs   => $lfn,
#		PFNs	   => $input->{RecoFile},
#		WNLocation => $input->{host} . ':' .
#			      $input->{dir}  . '/',
#      		RecoSize   => $input->{RecoSize},
#		status	   => $status,
#	      );
#      foreach ( qw / DataType SvcClass / )
#      {
#        if ( defined($input->{$_}) ) { $g{$_} = $input->{$_}; }
#      }
#      if ( $status ) { $g{RecoFailed} = delete $g{RecoReady}; }
#      $self->Log( \%g );
#    }
    $self->GatherStatistics($input);
    $self->CleanupReco($id);
  }

  elsif ( $command =~ m%Quit% )
  {
    Print "Quit: $command\n";
    my %text = ( 'command'   => 'Quit',
                 'client' => $client,
               );
    $heap->{client}->put( \%text );
  }

  else
  {
    Print "Unrecognised command: ",T0::Util::strhash($input),"\n";
  }
}

1;
