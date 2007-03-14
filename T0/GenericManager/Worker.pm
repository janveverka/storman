use strict;
package T0::GenericManager::Worker;
use POE;
use POE::Filter::Reference;
use POE::Component::Client::TCP;
use POE::Wheel::Run;
use Sys::Hostname;
use File::Basename;
use Cwd;
use T0::Util;
use T0::GenericManager::WorkerParser; 

our (@ISA, @EXPORT, @EXPORT_OK, %EXPORT_TAGS, $VERSION);
my $debug_me=1;

use Carp;
$VERSION = 1.00;
@ISA = qw/ Exporter /;

$GenericManager::Name = 'GenericManager::Worker-' . hostname();

my ($i,@queue);
our $hdr = __PACKAGE__ . ':: ';
sub Croak   { croak $hdr,@_; }
sub Carp    { carp  $hdr,@_; }
sub Verbose { T0::Util::Verbose( (shift)->{Verbose}, @_ ); }
sub Debug   { T0::Util::Debug(   (shift)->{Debug},   @_ ); }
sub Quiet   { T0::Util::Quiet(   (shift)->{Quiet},   @_ ); }

sub _init
{
  my $self = shift;

  $self->{Name} = $GenericManager::Name . '-' . $$;
  my %h = @_;
  map { $self->{$_} = $h{$_}; } keys %h;
  $self->ReadConfig();
  $self->{Host} = hostname();

  if ( defined($self->{Logger}) ) { $self->{Logger}->Name($self->{Name}); }

  POE::Component::Client::TCP->new
  ( RemotePort     => $self->{Manager}->{Port},
    RemoteAddress  => $self->{Manager}->{Host},
    Alias          => $self->{Name},
    Filter         => "POE::Filter::Reference",
    ServerError    => \&server_error,
    ConnectError   => \&_connection_error_handler,
    Disconnected   => \&_connection_error_handler,
    Connected      => \&_connected,
    ServerInput    => \&_server_input,
    InlineStates   => {
      got_child_stdout  => \&got_child_stdout,
      got_child_stderr  => \&got_child_stderr,
      got_child_close   => \&got_child_close,
      got_sigchld       => \&got_sigchld,
    },
    Args => [ $self ],
    ObjectStates   => [
	$self =>	[
				server_input => 'server_input',
				connected => 'connected',
      				job_done => 'job_done',
      				get_work => 'get_work',
			]
	],
  );

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

our @attrs = ( qw/ Name Host / );
our %ok_field;
for my $attr ( @attrs ) { $ok_field{$attr}++; }

sub AUTOLOAD {
  my $self = shift;
  my $attr = our $AUTOLOAD;
  $attr =~ s/.*:://;
  return unless $attr =~ /[^A-Z]/;  # skip DESTROY and all-cap methods
  Croak "AUTOLOAD: Invalid attribute method: ->$attr()" unless $ok_field{$attr};
  $self->{$attr} = shift if @_;
# if ( @_ ) { Croak "Setting attributes not yet supported!\n"; }
  return $self->{$attr};
}

sub ReadConfig
{
  my $self = shift;
  my $file = $self->{Config};

  return unless $file;
  $self->Log("Reading configuration file ",$file);

  $self->{Partners} = { Manager => 'GenericManager::Manager' };
  T0::Util::ReadConfig( $self, , 'GenericManager::Worker' );
# defined($self->{Processor}) or Croak "\"Processor\" not defined...\n";
}

sub Log
{ 
  my $self = shift;
  my $logger = $self->{Logger}; 
  defined $logger && $logger->Send(@_);
}

sub server_error { Print $hdr," Server error\n"; }

sub _connection_error_handler { reroute_event( (caller(0))[3], @_ ); }
sub connection_error_handler
{
  my ( $self, $kernel ) = @_[ OBJECT, KERNEL ];

  return if $self->{OnError}(@_);

  my $retry = $self->{RetryInterval};
  defined($retry) && $retry>0 || return;

  if ( !$self->{Retries}++ )
  {
    Print $hdr," Connection retry every $retry seconds\n";
  }
  $kernel->delay( reconnect => $retry );
}

sub got_child_stdout {
  my ($heap, $stdout) = @_[ HEAP, ARG0 ];
  print $stdout,"\n";;
  push @{$heap->{stdout}}, $stdout;
}

sub got_child_stderr {
  my ( $heap, $stderr) = @_[ HEAP, ARG0 ];
  print "STDERR: ",$stderr,"\n";;
  push @{$heap->{stdout}}, "STDERR: $stderr";
}

sub got_child_close {
  my ( $self, $kernel, $heap ) = @_[ OBJECT, KERNEL, HEAP ];
}

sub got_sigchld
{
  my ( $self, $heap, $kernel, $child_pid, $status ) =
                        @_[ OBJECT, HEAP, KERNEL, ARG1, ARG2 ];
  my $pid = $heap->{program}->PID;
  if ( $child_pid == $pid )
  {
    $kernel->yield( 'job_done', [ pid => $pid, status => $status ] );
    delete $heap->{program};
  }
  return 0;
}

sub _server_input { reroute_event( (caller(0))[3], @_ ); }
sub server_input {
  my ( $self, $kernel, $heap, $input ) = @_[ OBJECT, KERNEL, HEAP, ARG0 ];
  my ( $command, $client, $setup, $work, $priority );

  $command  = $input->{command};
  $client   = $input->{client};

  $self->Verbose("from server: $command\n");
  if ( $command =~ m%Sleep% )
  {
    $kernel->delay_set( 'get_work', $input->{wait} );
    return;
  }

  if ( $command =~ m%DoThis% )
  {
    my $cmd = $self->{Processor};

    $self->{MaxTasks}--;
    $work     = $input->{work};
    map { $cmd .= ' --' . $_ . '=' . $work->{$_} if m%^[A-Z]% } keys %$work;
    $self->Quiet("Execute $cmd\n");
    $kernel->sig( CHLD => "got_sigchld" );
    delete $heap->{stdout};
    $heap->{program} = POE::Wheel::Run->new
      ( Program      => $cmd,
        StdioFilter  => POE::Filter::Line->new(),
        StderrFilter => POE::Filter::Line->new(),
        StdoutEvent  => "got_child_stdout",
        StderrEvent  => "got_child_stderr",
        CloseEvent   => "got_child_close",
      );

    return;
  }

  if ( $command =~ m%Setup% )
  {
    $self->Quiet("Got $command...\n");
    $setup = $input->{setup};
    $self->{Debug} && dump_ref($setup);
    map { $self->{$_} = $setup->{$_} } keys %$setup;
    return;
  }

  if ( $command =~ m%Start% )
  {
    $self->Quiet("Got $command...\n");
    $kernel->yield('get_work');
    return;
  }

  if ( $command =~ m%Quit% )
  {
    $self->Quiet("Got $command...\n");
    $kernel->yield('shutdown');
    return;
  }

  Print "Error: unrecognised input from server! \"$command\"\n";
  $kernel->yield('shutdown');
}

sub _connected { reroute_event( (caller(0))[3], @_ ); }
sub connected
{
  my ( $self, $heap, $kernel, $input ) = @_[ OBJECT, HEAP, KERNEL, ARG0 ];
  $self->Debug("handle_connect: from server: $input\n");
  my %text = (  'command'       => 'HelloFrom',
                'client'        => $self->{Name},
             );
  $heap->{server}->put( \%text );
}

sub get_work
{
  my ( $self, $heap, $kernel ) = @_[ OBJECT, HEAP, KERNEL ];

  if ( ! defined($heap->{server}) )
  {
    $self->Verbose("No server! Wait a while...\n");
    $kernel->delay_set( 'get_work', 3 );
  }

  $self->Debug("Tasks remaining: ",$self->{MaxTasks},"\n");
  if ( $self->{MaxTasks} > 0 )
  {
    $heap->{WorkRequested} = time;
    my %text = ( 'command'      => 'SendWork',
                 'client'       => $self->{Name},
                );
    $heap->{server}->put( \%text );
  }
  else
  {
    Croak "This is not a good way to go...\n";
  }
}

sub client_input {
  my ( $heap, $input ) = @_[ HEAP, ARG0 ];
  Print "client_input: from server: $input\n";
  Croak "Do I ever get here...?\n";
}

sub job_done
{
  my ( $self, $heap, $kernel, $arg0 ) = @_[ OBJECT, HEAP, KERNEL, ARG0 ];
  my %h = @{ $arg0 };

  map { $h{$_} = $heap->{Work}->{$h{pid}}->{work}->{$_}; }
		keys %{$heap->{Work}->{$h{pid}}->{work}};

##########################################################################################
# Hacked by Pavel - parsing output and sending a notification
  
  if (defined $self->{ParseOutput} && $self->{ParseOutput}) {

    my %m;
    my $s = join(' ********* ',@{$heap->{stdout}});
    
    map {print "$_\n"} @{$heap->{stdout}};
    #print "$s\n";

    T0::GenericManager::WorkerParser::parse_output(\%m, $s);
    $self->Log(\%m);

  }; 
##########################################################################################

  $self->Quiet("Send: JobDone: work=$h{pid}, status=$h{status}, priority=$h{priority}\n");
  $h{priority} && $self->Log("JobDone: status=$h{status} priority=$h{priority}");
  $h{priority} && $self->Log(\%h);

# priority-zero tasks don't count against my total!
  if ( !$h{priority} ) { $self->{MaxTasks}++; }

  if ( $self->{MaxTasks} > 0 )
  {
    $kernel->yield( 'get_work' );
  }

  if ( $self->{MaxTasks} <= 0 )
  {
    Print "Shutting down...\n";
    $kernel->yield('shutdown');
  }
}

1;
