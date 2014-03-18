use strict;
package T0::RepackManager::Manager;
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
$RepackManager::Name = 'Repack::Manager';

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

  $self->{Name} = $RepackManager::Name;
  my %h = @_;
  map { $self->{$_} = $h{$_}; } keys %h;
  $self->ReadConfig();
  check_host( $self->{Host} ); 

  foreach ( qw / DeleteIndexFiles DeleteRawFiles
		 SegmentTimeout DatasetTimeout / )
  {
    $self->{$_} = 0 unless defined $self->{$_};
  }
  Croak "undefined Application\n" unless defined $self->{Application};
  Croak "Undefined operation mode\n" unless defined $self->{RepackMode};

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
	        SetSegmentTimer => 'SetSegmentTimer',
	         SegmentIsStale => 'SegmentIsStale',
	      SegmentIsComplete => 'SegmentIsComplete',
        SegmentHasBeenProcessed => 'SegmentHasBeenProcessed',
	        SetPayloadTimer => 'SetPayloadTimer',
	         PayloadIsStale => 'PayloadIsStale',
	      PayloadIsComplete => 'PayloadIsComplete',
        DatasetHasBeenProcessed => 'DatasetHasBeenProcessed',
		 ],
	],
    Args => [ $self ],
  );

  $self->{Queue} = POE::Queue::Array->new();
  undef $self->{dataset}{ID};
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

our @attrs = ( qw/ Name Host Port ConfigRefresh Config / );
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

sub PayloadIsComplete
{
  my ( $self, $kernel, $heap ) = @_[ OBJECT, KERNEL, HEAP ];
  my ($h,$g,$l,$lid,$file,@conf,$time,$did);
# This is a stub for something that actually does the work...
  my ($priority, $id, $work) = $self->{Queue}->dequeue_next();
  $did = $work->{Dataset};
  $h = $self->{dataset}{ID}{$did};
  $self->Quiet("Dataset $did is complete\n");
}

sub SetPayloadTimer
{
  my ( $self, $kernel, $heap, $id ) = @_[ OBJECT, KERNEL, HEAP, ARG0 ];
  $self->{_queue}{$id}{Start} = time;
  return unless $self->{DatasetTimeout};
  my $delay = $kernel->delay_set('PayloadIsStale',$self->{DatasetTimeout},$id);
  $self->Verbose("SetPayloadTimer: Delay ID: $delay Payload ID: $id\n");
  $self->{dataset}{DelayID}{$id} = $delay;
}

sub PayloadIsStale
{
  my ( $self, $kernel, $heap, $id ) = @_[ OBJECT, KERNEL, HEAP, ARG0 ];
  my ($x,$lid,$did);
  $self->Verbose("Check if Payload $id is stale...\n");
  return unless defined($self->{_queue}{$id});
  $x = $self->{_queue}{$id};
  my $age = time - $x->{Start};
  $self->Quiet("Payload $id is stale (age: $age seconds)\n");
  $self->CleanupPayload($id);
}

sub CleanupPayload
{
  my ($self,$id) = @_;
  my ($x,$lid,$did);
  $x = $self->{_queue}{$id};
  $did = $x->{Dataset};
  foreach $lid ( @{$x->{Segments}} )
  {
    if ( ! defined($lid) || ! defined($self->{lumi}{$lid}) )
    {
      $DB::single=$debug_me;
    }
    my $count = $self->{lumi}{$lid}{Datasets}{$did};
    $self->Quiet("Payload $id: Lumi $lid: Dataset $did: Count $count\n");
    delete $self->{lumi}{$lid}{Datasets}{$did};
    my $i = scalar keys %{$self->{lumi}{$lid}{Datasets}};
    if ( $i )
    {
      $self->Quiet("Payload $id: Lumi $lid: Datasets left: $i\n");
    }
    else
    {
      $self->Quiet("LumiID $lid is complete, can delete it!\n");
      $self->DeleteSegment($lid);
    }
  }
  delete $self->{_queue}{$id};
}

sub DatasetHasBeenProcessed
{
  my ( $self, $kernel, $heap, $did ) = @_[ OBJECT, KERNEL, HEAP, ARG0 ];
# my $h = $self->{lumi}{$did};
Carp "DatasetHasBeenProcessed: Not yet written...\n";
  my ($type,$file);
  $self->Quiet("Payload $did has been processed\n");
  $self->CleanupPayload($did);
}

sub SegmentIsComplete
{
  my ( $self, $kernel, $heap, $lid ) = @_[ OBJECT, KERNEL, HEAP, ARG0 ];
  my ($file,$did,$size,$priority);
  $self->Quiet("Lumi segment $lid is complete\n");

# Now to extract the dataset pieces, and see what can be processed!
  foreach $file ( @{$self->{lumi}{$lid}{Files}{idx}} )
  {
    open IDX, "<$file" or Croak "open $file: $!\n";
    $_ = <IDX> ; # Read and discard first line
    while ( <IDX> )
    {
      m%(\d+)\s+(\d+)\s+(\d+)% or Croak "Malformed line \"$_\" in $file\n";
      $did = $1;
      $size = $3;
      $self->{dataset}{ID}{$did}{Segments}{$lid} += $size;
      $self->{lumi}{$lid}{Datasets}{$did}++;
    }
    close IDX;
  }

  foreach $did ( sort { $a <=> $b } keys %{$self->{dataset}{ID}} )
  {
    $self->{dataset}{ID}{$did}{Size} +=
		 $self->{dataset}{ID}{$did}{Segments}{$lid};
    my $_mb = int($self->{dataset}{ID}{$did}{Size}/(1024*1024));
    $self->Verbose("Dataset $did: $_mb MB\n");

    if ( $self->{dataset}{ID}{$did}{Size} >= $self->{DatasetSize} )
    {
      my ($mb,@x,@y,%x,$priority,$id);
      $mb = int($self->{dataset}{ID}{$did}{Size}/(1024*1024));
      $self->Quiet("Dataset $did is complete ($mb MB)\n");
      @x = sort { $a <=> $b } keys %{$self->{dataset}{ID}{$did}{Segments}};
      $x{Segments} = \@x;
      foreach ( @x ) { push @{$x{Files}}, @{$self->{lumi}{$_}{Files}{idx}}; }
      $x{Dataset} = $did;
      $x{Protocol}  = $self->{TargetProtocol};
      $x{Target}  = $self->{SelectTarget}($self);
      if ( $x{Target} ne '/dev/null' )
      {
        $x{Target}  .= "/Export.$did." . join('_',@x) . '.' . uuid . '.raw';
      }
      $x{Size}	= $self->{dataset}{ID}->{$did}{Size};
      $priority	= 99; # $self->Priority(\%x); # Priority depends on dataset...
      $x{work}  = $self->{Application};
      $x{RepackMode}	= $self->{RepackMode};
      $x{Host}		= $self->{Host};
      $x{IndexProtocol}	= $self->{IndexProtocol};
      $id = $self->{Queue}->enqueue( $priority, \%x );
      $self->{_queue}{$id} = \%x;
      delete $self->{dataset}{ID}->{$did};
      $kernel->yield( 'SetPayloadTimer', $id );
    }
  }
}

sub SetSegmentTimer
{
  my ( $self, $kernel, $heap, $lid ) = @_[ OBJECT, KERNEL, HEAP, ARG0 ];
  return if exists $self->{lumi}{$lid}{DelayID};
  return unless $self->{SegmentTimeout};
  my $delay = $kernel->delay_set('SegmentIsStale',$self->{SegmentTimeout},$lid);
  $self->Verbose("Delay ID: $delay LumiID: $lid\n");
  $self->{lumi}{$lid}{DelayID} = $delay;
}

sub SegmentIsStale
{
  my ( $self, $kernel, $heap, $lid ) = @_[ OBJECT, KERNEL, HEAP, ARG0 ];
  $self->Quiet("Check if segment $lid is stale...\n");
  return unless defined($self->{lumi}{$lid});
  my $i = scalar keys %{$self->{lumi}{$lid}{Datasets}};
  if ( $i )
  {
    my $age = time - $self->{lumi}{$lid}{Start};
    $self->Quiet("Stale segment $lid: age: $age seconds: Datasets left: $i\n");
  }
  $self->DeleteSegment($lid);
}

sub SegmentHasBeenProcessed
{
  my ( $self, $kernel, $heap, $lid ) = @_[ OBJECT, KERNEL, HEAP, ARG0 ];
  my $h = $self->{lumi}{$lid};
  my ($type,$file);

Croak "Do I ever get here...?\n";
  my $latency = time - $self->{lumi}{$lid}{Start};
  $self->Quiet("Lumi segment $lid has been processed, latency $latency\n");
  my %t = ( MonaLisa	=> 1,
	    Cluster	=> $T0::System{Name},
	    Node	=> 'Repack',
	    LumiLatency	=> $latency,
	  );
  $self->Log( \%t );
  $self->DeleteSegment($lid);
  my @a = sort { $a <=> $b } keys %{$self->{lumi}};
  if ( scalar @a )
  {
    $self->Quiet("Still ",scalar @a," segments running, oldest is ",$a[0],"\n");
  }
  else
  {
    $self->Quiet("No other segments running...\n");
  }
}

sub DeleteSegment
{
  my ($self,$lid) = @_;
  my ($h,$type,$file,$rm);

  my $latency = time - $self->{lumi}{$lid}{Start};
  $self->Quiet("Lumi segment $lid, latency $latency\n");
  my %t = ( MonaLisa	=> 1,
	    Cluster	=> $T0::System{Name},
	    Node	=> 'Repack',
	    LumiLatency	=> $latency,
	  );
  $self->Log( \%t );

  foreach $type ( keys %{$self->{lumi}{$lid}{Files}} )
  {
    if ( ( $type =~ m%idx%i && !$self->{DeleteIndexFiles} ) ||
         ( $type =~ m%raw%i && !$self->{DeleteRawFiles} ) )
    {
      $self->Verbose("Not deleting \"$type\" files for segment $lid\n");
      next;
    }
    $self->Verbose("LumiID=$lid: Cleaning files of type $type\n");
    foreach $file ( @{$self->{lumi}{$lid}{Files}{$type}} )
    {
      $self->Verbose("LumiID=$lid:   Delete $file\n");
      if ( $file =~ m%/castor/% ) { $rm = 'stager_rm -M'; }
      else                        { $rm = 'rfrm'; }
      open RM, "$rm $file 2>&1 |" or Croak "$rm $file: $!\n";
      while ( <RM> )
      {
        $self->Debug("RM $file: $_");
      }
      close RM or $self->Debug("close RM $file: $!\n");
    }
  }
  delete $self->{lumi}{$lid};
  $self->Verbose("LumiID=$lid: Deleted!\n");
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

sub check_rate
{
  my ( $self, $kernel, $heap, $session ) = @_[  OBJECT, KERNEL, HEAP, SESSION ];
  $self->{StatisticsInterval} = 60 unless defined($self->{StatisticsInterval});
  $kernel->delay_set( 'check_rate', $self->{StatisticsInterval} );

  my ($i,$sum,$s,%h);
  $s = $self->{StatisticsInterval};
  $i = $sum = 0;
  while ( $_ = shift @{$self->{stats}} )
  {
    $sum+= $_;
    $i++;
  }
  $sum = int($sum*100/1024/1024)/100;
  $self->Debug("$sum MB in $s seconds, $i readings\n");
  %h = (     MonaLisa => 1,
	     Cluster  => $T0::System{Name},
             Node     => 'Repack',
             Rate     => $sum/$s,
             Readings => $i,
       );
  $self->Log( \%h );
}

sub GatherStatistics
{
  my $self = shift;
  my $i = shift or return;
  push @{$self->{stats}}, $i;
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
# _WHY_ do I need  to do this...?
  $kernel->state( 'SetSegmentTimer',		$self );
  $kernel->state( 'SegmentIsStale',		$self );
  $kernel->state( 'SegmentIsComplete',		$self );
  $kernel->state( 'SegmentHasBeenProcessed',	$self );
  $kernel->state( 'SetPayloadTimer',		$self );
  $kernel->state( 'PayloadIsStale',		$self );
  $kernel->state( 'PayloadIsComplete',		$self );
  $kernel->state( 'DatasetHasBeenProcessed',	$self );

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
  my %text = ( 'command' => 'Setup',
               'setup'   => \%Repack::Worker,
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

  $self->{Partners} = { Worker => 'Repack::Worker' };
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
#  my ( $self, $kernel, $heap, $session, $client ) =
#			@_[ OBJECT, KERNEL, HEAP, SESSION, ARG0 ];
#  $self->Verbose($session->ID,": $client: handle_unfinished\n");
#  my $q = $self->Queue($client);
#  return unless $q;
#  my ($p,$i,$w) = $q->dequeue_next();
#  while ( $i )
#  {
#    $self->Quiet("Pending Task: Client=$client, work=$w, priority=$p\n");
#    ($p,$i,$w) = $q->dequeue_next();
#  };
#    
#  delete $heap->{client};
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
               'setup'   => \%Repack::Worker,
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
# get some generic work...
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
      eval { $heap->{client}->put( \%text ); };
      if ( $@ )
      {
        Print "$client: Error sending work: $@\n";
        delete $heap->{client};
        $self->RemoveClient($client);
      }
      return;
    }
    $heap->{idle} = 0;
  }
  else
  {
    ($priority, $id, $work) = $self->{Queue}->dequeue_next();
    if ( ! $id )
    {
      $self->Debug("Idling ",$heap->{idle}++," times\n");
      $kernel->delay_set('send_work',7);
      return;
    }
  }
  $self->Quiet("Send: work=$work, priority=$priority to $client\n");
  %text = ( 'command'	=> 'DoThis',
            'client'	=> $client,
            'work'	=> $work,
	    'priority'	=> $priority,
	    'id'	=> $id,
           );
  eval { $heap->{client}->put( \%text ); };
  if ( $@ )
  {
    Print "$client: Error sending work: $@\n";
    delete $heap->{client};
    $self->RemoveClient($client);
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
    my $id       = $input->{work}{id};
    $self->Quiet("JobDone: work=$work, priority=$priority, id=$id, status=$status\n");

#   Check rate statistics from the first client onwards...
    if ( !$self->{client_count}++ ) { $kernel->yield( 'check_rate' ); }

    if ( $work->{work}{Target} )
    {
      my %h = (	MonaLisa	=> 1,
		Cluster		=> $T0::System{Name},
		Node		=> 'Repack',
		QueueLength	=> scalar keys %{$self->{_queue}},
		NRepackers	=> scalar keys %{$self->{clients}},
	      );
      if ( exists($self->{_queue}{$id}{Start}) )
      {
        $h{Duration} = time - $self->{_queue}{$id}{Start};
      }
      $self->Log( \%h );
      my %g = ( ExportReady => $work->{work}{Target} );
      my $size;
      foreach ( @{$input->{stderr}} )
      {
        if ( m%checksum\s+(\d+)% ) { $g{Checksum} = $1; }
        if ( m%wrote\s+(\d+)%    ) { $g{Size} = $size = $1; }
      }
      $self->Log( \%g );
      $self->GatherStatistics($size) if defined($size);
    }
    $self->CleanupPayload($id);
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
