use strict;
use warnings;
package T0::Iterator::Rfdir;
use POE qw( Wheel::Run Filter::Line );
use Date::Manip;

our (@ISA, @EXPORT, @EXPORT_OK, %EXPORT_TAGS, $VERSION);

use Carp;
$VERSION = 1.00;
@ISA = qw/ Exporter /;

our $hdr = __PACKAGE__ . ':: ';
sub Croak   { croak $hdr,@_; }
sub Carp    { carp  $hdr,@_; }
sub Verbose { T0::Util::Verbose( (shift)->{Verbose}, @_ ); }
sub Debug   { T0::Util::Debug(   (shift)->{Debug},   @_ ); }
sub Quiet   { T0::Util::Quiet(   (shift)->{Quiet},   @_ ); }

sub new {
  my ($class, $hash_ref) = @_;
  my $self = {};
  bless($self, $class);

  POE::Session->create(
		       inline_states => {
					 _start => \&start_tasks,
					 start_tasks => \&start_tasks,
					 got_task_stdout	=> \&got_task_stdout,
					 got_task_stderr	=> \&got_task_stderr,
					 got_task_close	=> \&got_task_close,
					 got_sigchld	=> \&got_sigchld,
					},
		       args => [ $hash_ref ],
		      );

  return $self;
}

sub start_tasks {
  my ( $kernel, $heap, $hash_ref ) = @_[ KERNEL, HEAP, ARG0 ];

  # save parameters on heap
  $heap->{Session} = $hash_ref->{Session};
  $heap->{Callback} = $hash_ref->{Callback};
  $heap->{MinAge} = $hash_ref->{MinAge};
  $heap->{Files} = $hash_ref->{Files};

  # register signal handler
  $kernel->sig( CHLD => "got_sigchld" );

  $heap->{WheelCount} = 1;

  my $task = POE::Wheel::Run->new(
				  Program => [ "rfdir", $hash_ref->{Directory} ],
				  StdoutFilter => POE::Filter::Line->new(),
				  StdoutEvent  => "got_task_stdout",
				  StderrEvent  => "got_task_stderr",
				  CloseEvent   => "got_task_close",
				 );

  $heap->{task}->{ $task->ID } = $task;
  $heap->{directory}->{ $task->ID } = $hash_ref->{Directory};
}

sub got_task_stdout {
  my ( $kernel, $heap, $stdout, $task_id ) = @_[ KERNEL, HEAP, ARG0, ARG1 ];

  my $currentDirectory = $heap->{directory}->{ $task_id };

  chomp($stdout);

  # parse line
  my @temp = split (" ", $stdout);

  my $protection = $temp[0];
  my $size = $temp[4];
  my $date = $temp[5] . " " . $temp[6] . " " . $temp[7];
  my $file = $temp[8];

  my $filename = $currentDirectory . '/' . $file;

  if ( $protection =~ /^dr/ && ! ( $file =~ /^\./ ) )
    {
      $heap->{WheelCount}++;

      my $task = POE::Wheel::Run->new(
				      Program => [ "rfdir", $filename ],
				      StdoutFilter => POE::Filter::Line->new(),
				      StdoutEvent  => "got_task_stdout",
				      StderrEvent  => "got_task_stderr",
				      CloseEvent   => "got_task_close",
				     );

      $heap->{task}->{ $task->ID } = $task;
      $heap->{directory}->{ $task->ID } = $filename;
    }
  elsif ( $protection =~ /^-r/ )
    {
      if ( not exists $heap->{Files}->{Status}->{$filename} )
	{
	  my $parsedDate = ParseDate($date);
	  my $secondsSinceEpoch = UnixDate($parsedDate,"%s");

	  # check that fileDate is earlier than cutoffDate (only if MinAge is defined)
	  my $flag = -1;
	  if ( defined $heap->{MinAge} )
	    {
	      $flag = Date_Cmp( ParseDate($date), DateCalc("now","- " . $heap->{MinAge} . " minutes") );
	    }
	  if ( $flag < 0 )
	    {
	      $heap->{Files}->{Status}->{$filename} = 0;
	      if ( exists $heap->{Files}->{Size} ) { $heap->{Files}->{Size}->{$filename} = $size; }
	      if ( exists $heap->{Files}->{Date} ) { $heap->{Files}->{Date}->{$filename} = $secondsSinceEpoch; }
	    }
	}
    }
}

sub got_task_stderr {
    my $stderr = $_[ARG0];
    print "RFDIR STDERR: $stderr\n";
}

sub got_task_close {
  my ( $kernel, $heap, $task_id ) = @_[ KERNEL, HEAP, ARG0 ];

  $heap->{WheelCount}--;

  #
  # check if all wheels have finished
  #
  if ( $heap->{WheelCount} == 0 )
    {
      $kernel->post($heap->{Session},$heap->{Callback});
    }

  delete $heap->{task}->{$task_id};
  delete $heap->{directory}->{$task_id};
}

sub got_sigchld {
  return 0;
}


1;
