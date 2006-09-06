use strict;
package T0::RepackManager::Worker;
use POE;
use POE::Filter::Reference;
use POE::Component::Client::TCP;
use POE::Wheel::Run;
use Sys::Hostname;
use T0::Util;

our (@ISA, @EXPORT, @EXPORT_OK, %EXPORT_TAGS, $VERSION);

use Carp;
$VERSION = 1.00;
@ISA = qw/ Exporter /;

$RepackManager::Name = 'Repack::Worker-' . hostname();

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

  $self->{Name} = $RepackManager::Name . '-' . $$;
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
    Connected      => \&_connected,
    ServerInput    => \&_server_input,
    InlineStates   => {
      got_child_stdout	=> \&got_child_stdout,
      got_child_stderr	=> \&got_child_stderr,
      got_child_close	=> \&got_child_close,
      got_sigchld	=> \&got_sigchld,
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

our @attrs = ( qw/ Name Host ConfigRefresh Config / );
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

  $self->{Partners} = { Manager => 'Repack::Manager' };
  T0::Util::ReadConfig( $self, , 'Repack::Worker' );

# $self->{Interval} = $self->{Interval} || 10;
}

sub Log
{ 
  my $self = shift;
  my $logger = $self->{Logger}; 
  defined $logger && $logger->Send(@_);
}

sub got_child_stdout {
  my ($self, $heap, $stdout) = @_[ OBJECT, HEAP, ARG0 ];
  push @{$heap->{stdout}}, $stdout;
# $self->Quiet("STDOUT: $stdout\n");
}

sub got_child_stderr {
  my ($self, $heap, $stderr) = @_[ OBJECT, HEAP, ARG0 ];
  push @{$heap->{stderr}}, $stderr;
  $stderr =~ tr[ -~][]cd;
  Print "STDERR: $stderr\n";
}

sub got_child_close {
  my ( $self, $kernel, $heap ) = @_[ OBJECT, KERNEL, HEAP ];
# if ( $self->{Debug} )
# {
#   my $work = $heap->{program}[$heap->{program}->PROGRAM];
#   Print "child closed: $work, ",$heap->{Work}->{$work},"\n";
# }
}

sub got_sigchld
{
  my ( $self, $heap, $kernel, $child_pid, $status ) =
			@_[ OBJECT, HEAP, KERNEL, ARG1, ARG2 ];
  my $pid = $heap->{program}->PID;
# $self->Debug("sig_chld handler: PID=$child_pid, RC=$status Prog=$work\n");
  $kernel->yield( 'job_done', [ pid => $pid, status => $status ] );
  if ( $child_pid == $pid ) {
    delete $heap->{program};
    delete $heap->{stdio};
  }
  return 0;
}

sub PrepareConfigFile
{
  my ($self,$h) = @_;
  my %modes = ( 'Classic' => 0,
		'LocalPush' => 0,
                'LocalPull' => 0,
	      );

  if ( !defined($h->{RepackMode}) ||
       !defined($modes{$h->{RepackMode}}) )
  {
    Croak "RepackMode not valid. Use one of \"",join('", "',keys %modes),"\"\n";
  }
#  my $conf = "./repack.export." . $h->{Dataset} . '.' .
#	  join('_',@{$h->{Segments}}) . '.conf';
  my $conf = "./repack.export." . $h->{Dataset} . '.' .
	  $h->{Segments}->[0] . '_' . $h->{Segments}->[-1] . '.conf';
  Print "Creating \"$conf\"\n";
  open CONF, ">$conf" or die "open: $conf: $!\n";
  print CONF "SelectStream = ",$h->{Dataset},"\n";
  my $protocol = $h->{Protocol};
  if ( $protocol eq 'rfio:' && $h->{Target} =~ m%^/castor% )
    {
      $protocol = "rfio:///?svcClass=" . $self->{SvcClass} . "&path=";
    }
  else
    {
      $protocol = "rfio:";
    }

  print  CONF "OpenFileURL = $protocol$h->{Target}\n";
  foreach ( @{$h->{Files}} )
  {
    if ( ( $self->{Host} ne $h->{Host} ) && defined($h->{IndexProtocol}) )
    {
      print  CONF "IndexFile = ",$h->{IndexProtocol},$h->{Host},":$_\n";
    }
    else
    {
      print  CONF "IndexFile = $_\n";
    }
  }
  print  CONF "CloseFileURL = $protocol$h->{Target}\n";
  close CONF;
  return $conf;
}

sub _server_input { reroute_event( (caller(0))[3], @_ ); }
sub server_input {
  my ( $self, $kernel, $heap, $input ) = @_[ OBJECT, KERNEL, HEAP, ARG0 ];
  my ( $command, $client, $setup, $work, $priority );

  $command  = $input->{command};
  $client   = $input->{client};

  $self->Verbose("from server: $command\n");
  if ( $command =~ m%DoThis% )
  {
    $work     = $input->{work};
    $priority = $input->{priority};
    $priority = 99 unless defined($priority);

    my %h = ( MonaLisa  => 1,
	      Cluster	=> $T0::System{Name},
              Node      => 'Repack',
              IdleTime	=> time - $heap->{WorkRequested}
	    );
    $self->Log( \%h );
    $self->Quiet("Got $command($work,$priority)...\n");

    my $c = $work->{work} . ' ' . $self->PrepareConfigFile($work);
    $heap->{program} = POE::Wheel::Run->new
      ( Program	     => $c,
        StdioFilter  => POE::Filter::Line->new(),
        StderrFilter => POE::Filter::Line->new(),
        StdoutEvent  => "got_child_stdout",
        StderrEvent  => "got_child_stderr",
        CloseEvent   => "got_child_close",
      );
    $kernel->sig( CHLD => "got_sigchld" );
    $heap->{Work}->{$heap->{program}->PID} = $input;

    return;
  }

  if ( $command =~ m%Setup% )
  {
    $self->Quiet("Got $command...\n");
    $setup = $input->{setup};
    $self->{Debug} && dump_ref($setup);
    map { $self->{$_} = $setup->{$_} } keys %$setup;
    $kernel->yield('get_work');
    return;
  }

  if ( $command =~ m%Start% )
  {
    $self->Quiet("Got $command...\n");
#   $kernel->yield('get_work');
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
  if ( $self->{MaxTasks}-- > 0 )
  {
    $heap->{WorkRequested} = time;
    my %text = ( 'command'      => 'SendWork',
                 'client'       => $self->{Name},
                );
    $heap->{server}->put( \%text );
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

  $h{priority} = $heap->{Work}->{$h{pid}}->{priority};

  $self->Quiet("Send: JobDone: work=$h{pid}, status=$h{status}, priority=$h{priority}\n");
  $h{priority} && $self->Log("JobDone: status=$h{status} priority=$h{priority}");
  $h{priority} && $self->Log(\%h);
  if ( defined($heap->{server}) )
  {
    $h{command} = 'JobDone';
    $h{client}  = $self->{Name};
    $h{work}    = $heap->{Work}->{$h{pid}};
    $h{stdout}  = $heap->{stdout};
    $h{stderr}  = $heap->{stderr};
    $heap->{server}->put( \%h );
    delete $heap->{Work}->{$h{pid}};
    delete $heap->{stdout};
    delete $heap->{stderr};
  }
  else
  {
    Print "Woah, server left me! Couldn't send ",join(', ',%h),"\n";
  }

  my $w = scalar(keys %{$heap->{Work}});
  $self->Verbose("JobDone: tasks left=",$self->{MaxTasks}," queued=$w\n");

# priority-zero tasks don't count against my total!
  if ( !$h{priority} ) { $self->{MaxTasks}++; }

  if ( $self->{MaxTasks} > 0 )
  {
    $kernel->yield( 'get_work' );
  }

  if ( $self->{MaxTasks} <= 0 && ! $w )
  {
    Print "Shutting down...\n";
    $kernel->yield('shutdown');
  }
}

1;
