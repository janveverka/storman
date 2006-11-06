use strict;
package T0::Component::Worker;
use POE;
use POE::Filter::Reference;
use POE::Component::Client::TCP;
use POE::Wheel::Run;
use Sys::Hostname;
use File::Basename;
use Cwd;
use T0::Util;

our (@ISA, @EXPORT, @EXPORT_OK, %EXPORT_TAGS, $VERSION);
my $debug_me=1;
our %known_dirs;

use Carp;
$VERSION = 1.00;
@ISA = qw/ Exporter /;

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

  $self->{pwd} = cwd;
  $self->{State}	 = 'Created';
  $self->{QueuedThreads} = 0;
  $self->{ActiveThreads} = 0;
  $self->{MaxThreads}	 = 1;

  my %h = @_;
  map { $self->{$_} = $h{$_}; } keys %h;
  defined($self->{Name}) or Croak "Shamed, unnamed, I die...\n";
 ($self->{Node} = $self->{Name} ) =~ s%::.*$%%;
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
    InlineStates   => {
      got_child_stdout	=> \&got_child_stdout,
      got_child_stderr	=> \&got_child_stderr,
      got_child_close	=> \&got_child_close,
      got_sigchld	=> \&got_sigchld,
    },
    Args => [ $self ],
    ObjectStates   => [
	$self =>	[
				server_input	=> 'server_input',
				connected	=> 'connected',
		       connection_error_handler => 'connection_error_handler',
      				job_done	=> 'job_done',
      				get_work	=> 'get_work',
				Quit		=> 'Quit',
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
  return $self->{$attr};
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
  defined($self->{CfgTemplate}) or Croak "\"CfgTemplate\" not defined...\n";
  if ( $self->{CfgTemplate} !~ m%.tmpl$% )
  {
    Croak "\"CfgTemplate\" has no \".tmpl\" suffix...\n";
  }

  map { $self->{Channels}->{$_} = 1; } @{$T0::System{Channels}};
  SetProductMap;
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

sub FlushQueue
{
  my $self = shift;
  my $heap = shift;
  while ( $_ = shift @{$self->{Queue}} )
  {
    $self->Debug("Draining queue: ",$_,"\n");
    $heap->{server}->put($_);
  }
}

sub send
{
  my ( $self, $heap, $ref ) = @_;
  if ( !ref($ref) ) { $ref = \$ref; }
  if ( $heap->{connected} && $heap->{server} )
  {
    $self->FlushQueue($heap);
    $heap->{server}->put( $ref );
  }
  else { push @{$self->{Queue}}, $ref; }
}

sub Log
{ 
  my $self = shift;
  my $logger = $self->{Logger}; 
  defined $logger && $logger->Send(@_);
}

sub got_child_stdout {
  my ($heap, $stdout) = @_[ HEAP, ARG0 ];
  print LOGOUT $stdout,"\n";;
  my $work = $heap->{Work}->{$heap->{program}->PID};

# This is not useful...
#  if ( $stdout =~ s%^T0PoolFileCatalog:\s+%% )
#  {
#    if ( $stdout =~ m%File ID="([^"]+)"% ) { $work->{FileID} = $1; }
#  }
  if ( $stdout =~ s%^T0Signature:\s+%% )
  {
    Print $stdout,"\n";
    while ( $stdout =~ s%\s*([^=]+)=(\S+)\s*%% ) { $work->{$1} = $2; }
  }
  if ( $stdout =~ m%^T0Checksums:\s+(\d+)\s+(\d+)\s+(\S+)% )
  {
    my $f = $3;
    $work->{Files}->{$f}->{Size} = $2;
    $work->{Files}->{$f}->{Checksum} = $1;
    my $r = $f;
    $r =~ s%^.*\.(.+)\.root%$1%;
    $work->{Files}->{$f}->{DataType} = $r;
  }

  my ($run,$evt);

  if ( $stdout =~ m/^Begin processing the \S+ record. Run (\d+), Event (\d+)/ )
  {
    $run = $1; $evt = $2;
  }
  if ( $stdout =~ m/^TimeModule>\s*(\d+)\s+(\d+)/g )
  {
    $run = $2; $evt = $1;
  }
  if ( defined($run) && defined($evt) )
  {
    next if $heap->{events}{$run}{$evt}++;
#   push @{$heap->{stdout}}, $stdout;
    my $nevt = ++$work->{NbEvents};
    my $freq = $heap->{self}->{ReportFrequency} || 50;
    if ( $nevt == 1 || $nevt == 2 || $nevt == 5 or ($nevt%$freq) == 0 )
    {
      my $file = $work->{dir} .'/' . $work->{RecoFile};
      my $size = (stat($file))[7] / 1024 / 1024;
      $heap->{self}->Quiet('NbEvents ',$work->{NbEvents},' RecoSize ',$size,"\n");
      $heap->{self}->{Dashboard}->Send( 'NEvents', $work->{NbEvents},
					'RecoSize', $size );
    }
  }
}

sub got_child_stderr {
  my ( $heap, $stderr) = @_[ HEAP, ARG0 ];
# push @{$heap->{stderr}}, $stderr;
  $stderr =~ tr[ -~][]cd;
  print LOGOUT "STDERR: ",$stderr,"\n";
  #$heap->{self}->Verbose("STDERR: $stderr\n");
# if ( $stderr =~ m%Run:\s+(\d+)\s+Event:\s+(\d+)% )
# {
#   push @{$heap->{stderr}}, $stderr;
# }
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
  return unless defined($heap->{program});
  my $pid = $heap->{program}->PID;
  if ( $child_pid == $pid ) {
    $kernel->yield( 'job_done', [ pid => $pid, status => $status ] );
    delete $heap->{program};
    delete $heap->{stdio};
    close LOGOUT or Croak "closing logfile: $!\n";
    delete $self->{Children}->{$pid};
  }
  return 0;
}

sub PrepareConfigFile
{
  my ($self,$h) = @_;
  my ($ifile,$ofile,$conf);

  my %protocols = ( 'Classic'	=> 'rfio:',
		    'LocalPush'	=> 'file:',
       		    'LocalPull'	=> 'file:',
		  );
  $ofile = basename $h->{File};
  $ofile =~ s%root$%%;
  $ofile .= $self->{DataType} . '.root';
  $ofile = SelectTarget($self) . '/' . $ofile;
  $h->{RecoFile} = $ofile;

  if ( defined($self->{DataDirs}) )
  {
    my %g = (
		'TargetDirs' => $self->{DataDirs},
		'TargetMode' => 'RoundRobin'
	    );
    my $dir = SelectTarget( \%g );
#   This is a hack while I fix the persistent queueing in the Manager
#   ...and it doesn't work anymore, because the output structure is wrong!
#    my $exists = 1;
#    my $f = $dir . '/' . $ofile;
#    open RFSTAT, "rfstat $f 2>&1 |" or Croak "rfstat $f: $!\n";
#    while ( <RFSTAT> )
#    { if ( m%No such file or directory% ) { $exists = 0; } }
#    close RFSTAT; # or Croak "close: rfstat $f: $!\n";
#    if ( $exists ) { return undef; }
  }

  $ifile = $h->{File};
  if ( $self->{Mode} eq 'LocalPull' )
  {
    my $cmd = "rfcp $h->{File} .";
    if ( $self->{InputSvcClass} )
    {
      $cmd = "STAGE_SVCCLASS=" . $self->{InputSvcClass} . ' ' . $cmd;
    }
    if ( ! open RFCP, "$cmd |" )
      {
	$self->Verbose("ERROR: can't rfcp $h->{File} !\n");
	return undef;
      }
    while ( <RFCP> ) { $self->Verbose($_); }
    if ( ! close RFCP )
      {
	$self->Verbose("ERROR: can't rfcp $h->{File} !\n");
	return undef;
      }
    $ifile = basename $ifile;
  }
  if ( $self->{Mode} eq 'LocalPush' )
  {
    $ifile = basename $ifile;
  }
  if ( $self->{Mode} ne 'Classic' )
  {
    -f $ifile or Croak "$ifile does not exist but mode is Local*\n";
  }

  $ifile = $protocols{$self->{Mode}} . $ifile;
  $self->Verbose("Input file : $ifile\n");

  $conf = $self->{CfgTemplate};
  $conf =~ s%^.*/%%;
  $conf =~ s%.tmpl$%%;
  Print "Creating \"$conf\"\n";
  open CONF, ">$conf" or Croak "open: $conf: $!\n";
  open TMPL, "<$self->{CfgTemplate}" or Croak "open: $self->{CfgTemplate}: $!\n";
  while ( <TMPL> )
  {
    s%T0_INPUT_FILE%$ifile%;
    s%T0_OUTPUT_FILE%$ofile%;
    s%T0_MAX_EVENTS%$self->{MaxEvents}%;
    print CONF;
  }
  close CONF;
  close TMPL;
  return $conf;
}

sub _server_input { reroute_event( (caller(0))[3], @_ ); }
sub server_input {
  my ( $self, $kernel, $heap, $input ) = @_[ OBJECT, KERNEL, HEAP, ARG0 ];
  my ( $command, $client, $setup, $work, $priority );

  $command  = $input->{command};
  $client   = $input->{client};

  $self->Verbose("from server: ",T0::Util::strhash($input),"\n");
  if ( $command =~ m%Sleep% )
  {
    $self->{QueuedThreads}-- if $self->{QueuedThreads};
    $self->Debug("Sleep: Queued threads: ",$self->{QueuedThreads},"\n");
    return;
  }

  if ( $command =~ m%DoThis% )
  {
    chdir $self->{pwd};
    $self->{MaxTasks}--;
    $self->{ActiveThreads}++;
    $work     = $input->{work};
    $priority = $input->{priority};
    $priority = 99 unless defined($priority);

    my %h = ( MonaLisa  => 1,
	      Cluster	=> $T0::System{Name},
              Node      => $self->{Node},
              IdleTime	=> time - $heap->{WorkRequested}
	    );
    $self->Log( \%h );
    $self->Quiet("Got $command: ",T0::Util::strhash($work)," ($priority)...\n");

    my $wdir = 'w_' . T0::Util::timestamp;
    mkdir $wdir or Croak "mkdir: $wdir: $!\n";
    chdir $wdir or Croak "chdir: $wdir: $!\n";

    $self->{Dashboard}->Step($work->{id});
    $self->{Dashboard}->Start('NEvents',0,'RecoSize',0);

    my $c = $self->PrepareConfigFile($work);
    unless (defined($c))
    {
      $self->Verbose("ERROR: skip this workload !\n");
      $kernel->yield( 'job_done', [ pid => -1, status => -1 , reason => 'Preparation failed (copying input file?)' ] );
      return;
    }
    $c = $work->{work} . ' ' . $c;
    $heap->{program} = POE::Wheel::Run->new
      ( Program	     => $c,
        StdioFilter  => POE::Filter::Line->new(),
        StderrFilter => POE::Filter::Line->new(),
        StdoutEvent  => "got_child_stdout",
        StderrEvent  => "got_child_stderr",
        CloseEvent   => "got_child_close",
      );
    $heap->{log} = 'log.PR.' . basename $work->{File};
    $heap->{log} =~ s%.root%%;
    $heap->{log} .= '.out.gz';
    $heap->{self} = $self;

    open LOGOUT, "| gzip - > $heap->{log}" or Croak "open: $heap->{log}: $!\n";


    $kernel->sig( CHLD => "got_sigchld" );
    my $cpid = $heap->{program}->PID;
    $heap->{Work}->{$cpid}{Parent} = $input->{work};
    $heap->{Work}->{$cpid}{dir}    = cwd;
    $heap->{Work}->{$cpid}{log}    = $heap->{log};
    $heap->{Work}->{$cpid}{host}   = $self->{Host};

#   Preserve some settings against config changes during the run
    foreach ( qw / LogDirs DataDirs InputSvcClass OutputSvcClass / )
    {
      $heap->{Work}->{$cpid}->{Setup}->{$_} = $self->{$_};
    }
    $self->{Children}->{$cpid}++;
    return;
  }

  if ( $command =~ m%Setup% )
  {
    $self->Quiet("Got $command...\n");
    $setup = $input->{setup};
    $self->{Debug} && dump_ref($setup);
    map { $self->{$_} = $setup->{$_} } keys %$setup;
#   $self->{QueuedThreads}-- if $self->{QueuedThreads};
    return;
  }

  if ( $command =~ m%SetState% )
  {
    my $state = $input->{SetState};
    $self->Quiet("Got State $state...\n");

    if ( $state =~ m%Resume% or $state =~ m%Start% )
    {
      $kernel->yield('get_work') if ( ! $self->{GettingWork}++ );
      $self->{State} = 'Running';
      return;
    }

    if ( $state =~ m%Pause% )
    {
      $self->{State} = $state;
      return;
    }

    if ( $state =~ m%Abort% )
    {
#     $self->{State} = $state;
      foreach ( keys %{$self->{Children}} )
      {
        $self->Quiet("Kill child PID=$_\n");
        kill 15 => -$_;
        delete $self->{Children}->{$_};
      }
      open KILL, "killall -KILL cmsRun |";
      while ( <KILL> ) { print; }
      close KILL;
      return;
    }

    if ( $state =~ m%Usr2% )
    {
      open KILL, "killall -USR2 cmsRun |";
      while ( <KILL> ) { print; }
      close KILL;
      return;
    }

    if ( $state =~ m%Flush% )
    {
      $self->{State} = $state;
      return;
    }

    if ( $state =~ m%Quit% or $state =~ m%WorkerQuit% )
    {
      $self->{State} = $state;
#     $kernel->yield('shutdown');
      $kernel->delay_set( 'Quit', 5 );
      return;
    }

    Print "Unrecognised state from server: ",T0::Util::strhash($input),"\n";
    return;
  }

  Print "Unrecognised input from server! ",T0::Util::strhash($input),"\n";
  $kernel->yield('shutdown');
}

sub _connected { reroute_event( (caller(0))[3], @_ ); }
sub connected
{
  my ( $self, $heap, $kernel, $input ) = @_[ OBJECT, HEAP, KERNEL, ARG0 ];

# Gotta put this somewhere...
  $kernel->state( 'Quit', $self );

  $self->Debug("handle_connect: from server: $input\n");
  my %text = (  'command'       => 'HelloFrom',
                'client'        => $self->{Name},
             );
  $self->send( $heap, \%text );
  $self->{QueuedThreads} = $self->{ActiveThreads};
}

sub get_work
{
  my ( $self, $heap, $kernel ) = @_[ OBJECT, HEAP, KERNEL ];

  $self->Debug("Queued threads: ",$self->{QueuedThreads},"\n");
  $kernel->delay_set( 'get_work', 7 ) if $self->{MaxTasks};
  return if ( $self->{State} ne 'Running' );
  return if ( $self->{QueuedThreads} >= $self->{MaxThreads} );
  $self->{QueuedThreads}++;

  $self->Verbose("Fetching a task: ",$self->{QueuedThreads},"\n");
  $self->Verbose("Tasks remaining: ",$self->{MaxTasks},"\n");
  if ( $self->{MaxTasks} > 0 )
  {
    $heap->{WorkRequested} = time;
    my %text = ( 'command'      => 'SendWork',
                 'client'       => $self->{Name},
                );
    $self->send( $heap, \%text );
  }
  else
  {
    Croak "This is not a good way to go...\n";
    exit 0;
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

# $h{Parent} = $heap->{Work}->{$h{pid}}->{Parent};
  map { $h{$_} = $heap->{Work}->{$h{pid}}->{$_}; }
		keys %{$heap->{Work}->{$h{pid}}};

# Pass some parameters to downstream components
  foreach ( qw / LogDirs DataDirs InputSvcClass OutputSvcClass / )
  {
    $h{$_} = $heap->{Work}->{$h{pid}}->{Setup}->{$_};
  }
  $h{SvcClass} = $h{OutputSvcClass}; # explicit change of key name.
  $h{Parent}{Dataset} = $h{Parent}{Channel} . $h{Parent}{DatasetNumber} unless $h{Parent}{Dataset};
  $h{Dataset} = $h{Parent}{Dataset} unless $h{Dataset};

#  $h{RecoSize} = 0;
#  if ( defined($h{dir}) && defined($h{RecoFile}) && -f $h{dir} . '/' . $h{RecoFile} )
#  {
#    $h{RecoSize} = (stat($h{dir} . '/' . $h{RecoFile}))[7] / 1024 / 1024;
#  }
#  else { $h{RecoSize} = 0; }

  $h{NbEvents} = GetEventsFromJobReport;
  my $x = GetRootFileInfo;
  $self->{Dashboard}->Stop($h{status},	$h{reason},
			   'NEvents',	$h{NbEvents},
			   'RecoSize',	$h{RecoSize});

  $self->Debug(T0::Util::strhash(\%h),"\n");
  my $lfndir;
  if ( defined($h{DataDirs}) )
  {
    my (%g,$dir,$file);
    my ($vsn,$datatype);
    my ($f,$g,$i,$stream);
    $vsn = $h{Version};
    $vsn =~ s%_%%g;

    foreach $file ( keys %{$x} )
    {
      my ($a,@a,$cmd,$pfn,$lfn,$key);
#     This was the coshure way of doing it, but it's not good enough for CSA06
      %g = ( 'TargetDirs' => $h{DataDirs},
	     'TargetMode' => 'RoundRobin' );
      $dir = SelectTarget( \%g );
      $f = $file;
      $f =~ s%^\.\/%%;
      $i = $x->{$file}{ID};
      $g = $i . '.root';

      $h{Parent}{Channel} = $h{Parent}{Parent}{Channel} unless defined $h{Parent}{Channel};
      $h{Channel} = $h{Parent}{Channel} unless defined $h{Channel};
      $stream = $x->{$f}{Module};
      $stream =~ s%_%%g; # Eliminate illegal characters
      if ( !IsRequiredProduct(
				$self->{DataType},
				$h{Channel},
				$stream
			     )
	 )
      {
        delete $x->{$f};
	unlink $f;
        next;
      }
      $x->{$g} = delete $x->{$f};
      $x->{$g}{OriginalFile} = $f;
      rename $f, $g;
      $h{Files}{$g} = delete $h{Files}{$f};
      map { $h{Files}{$g}{$_} = $x->{$g}{$_} } keys %{$x->{$g}};

#    %g = ( 'File'	=> $h{File},
#	   'Target'	=> $dir );
#    $dir = MapTarget( \%g, $self->{Channels} );
#
#   This is the CSA06-way. See the writeup at
#   https://twiki.cern.ch/twiki/bin/view/CMS/CMST0DataManagement
#   for details...
#
#     Primary dataset...
      my $pvsn = $h{Parent}{Version};
      $pvsn =~ s%_%%g;
      $pvsn = $vsn unless $pvsn;
      $lfndir = '/CSA06-' . $pvsn . '-os-' . $h{Channel} . '-0/';
#     Tier...
      $lfndir .= $self->{DataType};
#     Processing Name...
      $datatype = $self->{DataType};
      $lfndir .= '/CMSSW_' . $h{Version} . '-' . $stream . '-H' . $h{PsetHash};

#     Add a date-related subdirectory
      @a = localtime;
      $a = sprintf("%02i%02i",$a[4]+1,$a[3]);
      $lfndir .= '/' . $a;

      $dir .= $lfndir;
      if ( ! $known_dirs{$dir}++ )
      {
        open RFMKDIR, "rfmkdir -p $dir |" or warn "rfmkdir $dir: $!\n";
        while ( <RFMKDIR> ) {}
        close RFMKDIR; # No check for errors, I will have one if the dir exists
      }

      $pfn = $dir .'/' . $g;
      $lfn = $pfn;
      $lfn =~ s%^.*/store%/store%;
      delete $h{Parent}{Parent};
      my %u = (
		T0Name		=> $T0::System{Name},
		Sizes		=> $h{Files}{$g}{Size},
		CheckSums	=> $h{Files}{$g}{Checksum},
		DataType	=> $self->{DataType},
		PFNs		=> $pfn,
		RECOLFNs	=> $lfn,
		File		=> $g,
		WNLocation	=> $h{host} . ':' . $h{dir},
		Version		=> $h{Version},
		PsetHash	=> $h{PsetHash},
		Stream		=> $stream,
		Dataset		=> $h{Dataset},
 		NbEvents	=> $x->{$g}{NbEvents},
		Parent		=> $h{Parent},
		GUIDs		=> $h{Files}{$g}{ID},
	      );

      if ( $x->{$g}->{NbEvents} ) { $key = 'Ready'; }
      else                        { $key = 'Empty'; }
      if ( $x->{$g}{NbEvents} )
      {
        $cmd = "rfcp $g $dir";
        Print $cmd,"\n";
        if ( defined($h{OutputSvcClass}) )
        {
          $cmd = 'STAGE_SVCCLASS=' . $h{OutputSvcClass} . ' ' . $cmd;
        }
        open RFCP, "$cmd |" or Croak "$cmd: $!\n";
        while ( <RFCP> ) { $self->Verbose($_); }
        close RFCP or do
        {
          $u{status} = -1;
	  $u{reason} = 'Stageout failed';
          $key = 'Failed';
        };
      }
      $self->{AutoDelete} && unlink $g;
      $u{$self->{OutputKey} . $key} = 1;
      $self->Log( \%u );
    }
  }

  if ( defined($h{LogDirs}) )
  {
    my %g = ( 'TargetDirs' => $h{LogDirs},
	      'TargetMode' => 'RoundRobin' );
    my $dir = SelectTarget( \%g );
    %g = ( 'File'	=> $h{File},
           'Target'	=> $dir );

    $dir .= $lfndir;
    if ( ! $known_dirs{$dir}++ )
    {
      open RFMKDIR, "rfmkdir -p $dir |" or warn "rfmkdir $dir: $!\n";
      while ( <RFMKDIR> ) {}
      close RFMKDIR; # No check for errors, I will have one if the dir exists
    }
    if ( defined($dir) && defined($h{dir}) && defined($h{log}) && -f $h{dir} . '/' . $h{log} )
    {
      my $cmd = 'rfcp ' . $h{dir} . '/' . $h{log} . ' ' . $dir;
      Print $cmd,"\n";
      open RFCP, "$cmd |" or Croak "$cmd: $!\n";
      while ( <RFCP> ) { $self->Verbose($_); }
      close RFCP;

      if ( -f $h{dir} . '/FrameworkJobReport.xml' )
      {
        if ( ! defined $h{Parent}{GUIDs} )
        {
          my $guid = basename $h{Parent}{File};
          $guid =~ s%.root%%;
          $h{Parent}{GUIDs} = $guid;
        }
        $cmd = 'rfcp ' . $h{dir} . '/FrameworkJobReport.xml ' . $dir . '/FrameworkJobReport.' . $h{Parent}{GUIDs} . '.xml';
        Print $cmd,"\n";
        open RFCP, "$cmd |" or Croak "$cmd: $!\n";
        while ( <RFCP> ) { $self->Verbose($_); }
        close RFCP;
      }
    }
  }

# Kludges for now to get rid of the output and input...
$DB::single=$debug_me;
  $h{Parent}{PFNs} = $h{Parent}{File} unless defined $h{Parent}{PFNs};
  if ( defined($h{Parent}{PFNs}) )
  {
    my $xx = basename $h{Parent}{PFNs};
    if ( -f $xx )
    {
      $self->Verbose("Deleting ",$xx,"\n");
      unlink $xx;
    }
  }
# </kludge>

  $self->Quiet("Send: JobDone: work=$h{pid}, status=$h{status}, priority=$h{priority}\n");
  $h{priority} && $self->Log("JobDone: status=$h{status} priority=$h{priority}");
  $h{priority} && $self->Log(\%h);

  $h{command} = 'JobDone';
  $h{client}  = $self->{Name};
  $h{work}    = $heap->{Work}->{$h{pid}};
# $h{stdout}  = $heap->{stdout};
# $h{stderr}  = $heap->{stderr};
  $self->send( $heap, \%h );
  delete $heap->{Work}->{$h{pid}};
  delete $heap->{stdout};
  delete $heap->{stderr};
  delete $heap->{events};

  my $w = scalar(keys %{$heap->{Work}});
  $self->Verbose("JobDone: tasks left=",$self->{MaxTasks}," queued=$w\n");

  $self->Verbose("Active threads decremented: ",$self->{QueuedThreads},"\n");
  $self->{QueuedThreads}--;
  $self->{ActiveThreads}--;

  if ( $self->{MaxTasks} <= 0 && ! $w )
  {
    Print "Shutting down...\n";
    $kernel->yield('shutdown');
  }
}

sub Quit
{
  Print "I'm outta here...\n";
  exit 0;
}

1;
