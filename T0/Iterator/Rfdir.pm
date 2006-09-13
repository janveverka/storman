use strict;
use warnings;
package T0::Iterator::Rfdir;
use POE qw( Wheel::Run Filter::Line );
use Date::Manip;
use DB_File;

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


# files are keys, entries are
#
#   for fileStatusList :
#     0 for file ready to be inserted
#     1 for already injected file
#
#   for fileSizeList :
#     size of file
#
my %fileStatusList;
my %fileSizeList;

#
# database to make fileStatusList persistent
#
my $database = undef;


sub _init
{
  my $self = shift;

  my $heap = $_[HEAP];

  my %h = @_;
  map { $self->{$_} = $h{$_} } keys %h;
  $self->ReadConfig();

  POE::Session->create(
		       inline_states => {
					 _start => \&start_tasks,
					 start_tasks => \&start_tasks,
					 got_task_stdout	=> \&got_task_stdout,
					 got_task_stderr	=> \&got_task_stderr,
					 got_task_close	=> \&got_task_close,
					 got_sigchld	=> \&got_sigchld,
					},
		       args => [ $self->{Directory}, $self->{MinAge}, $self->{SleepTime} ],
		      );

  # if persistent, tie file to hash and clear hash of not injected files
  if ( defined $self->{Persistent} )
    {
      $database = tie(%fileStatusList, 'DB_File', $self->{Persistent}) || die "can't open $self->{Persistent}: $!";

      while ( my ($filename, $status) = each %fileStatusList )
	{
	  if ( 0 == $status )
	    {
	      delete($fileStatusList{$filename});
	    }
	}

      $database->sync();
    }

  return $self;
}

sub start_tasks
{
  my ( $kernel, $heap, $topDirectory, $minAge, $sleepTime ) = @_[ KERNEL, HEAP, ARG0, ARG1, ARG2 ];

  my $task = POE::Wheel::Run->new(
				  Program => [ "rfdir", $topDirectory ],
				  StdoutFilter => POE::Filter::Line->new(),
				  StdoutEvent  => "got_task_stdout",
				  StderrEvent  => "got_task_stderr",
				  CloseEvent   => "got_task_close",
				 );

  $heap->{MinAge} = $minAge;

  $heap->{task}->{ $task->ID } = $task;
  $heap->{directory}->{ $task->ID } = $topDirectory;
  $heap->{sleepTime}->{ $task->ID } = $sleepTime;
  $kernel->sig( CHLD => "got_sigchld" );

  $kernel->yield( 'test_delay' );
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

our @attrs = ( qw/ Config ConfigRefresh Directory MinAge SleepTime Persistent Rate / );
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
  no strict 'refs';
  my $self = shift;
  my $file = $self->{Config};
  return unless $file;

  T0::Util::ReadConfig( $self );
}

sub Next
{
  my $self = shift;

  # loop over files
  # return first uninjected
  while ( my ($filename, $status) = each %fileStatusList )
    {
      if ( 0 == $status )
	{
	  $fileStatusList{$filename} = 1;
	  if ( defined $database ) { $database->sync(); }
	  return ($filename,$fileSizeList{$filename}) if wantarray();
	  return $filename;
	}
    }

  return undef;
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
      my $task = POE::Wheel::Run->new(
				      Program => [ "rfdir", $filename ],
				      StdoutFilter => POE::Filter::Line->new(),
				      StdoutEvent  => "got_task_stdout",
				      StderrEvent  => "got_task_stderr",
				      CloseEvent   => "got_task_close",
				     );
      $heap->{task}->{ $task->ID } = $task;
      $heap->{directory}->{ $task->ID } = $filename;
      $kernel->sig( CHLD => "got_sigchld" );
    }
  elsif ( $protection =~ /^-r/ )
    {
      if ( not defined($fileStatusList{$filename}) )
	{
	  # check that fileDate is earlier than cutoffDate (only if MinAge is defined)
	  my $flag = -1;
	  if ( defined($heap->{MinAge}) )
	    {
	      $flag = Date_Cmp( ParseDate($date), DateCalc("now","- " . $heap->{MinAge} . " minutes") );
	    }
	  if ( $flag < 0 )
	    {
	      $fileStatusList{$filename} = 0;
	      $fileSizeList{$filename} = $size;
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

  #
  # check if this task runs in the top directory
  # sleepTime is only defined for this task, no other
  #
  # it's the users responsibility to choose a sensible sleep time !!!
  #
  my $sleepTime;
  if ( exists $heap->{sleepTime}->{ $task_id } )
    {
      $sleepTime = $heap->{sleepTime}->{ $task_id };

      if ( defined $sleepTime and $sleepTime > 0 )
	{
	  # start directory scan from the top again later
	  $kernel->delay( 'start_tasks', $sleepTime * 60, ( $heap->{directory}->{$task_id} , $heap->{MinAge} , $sleepTime ) );
	}

      delete $heap->{sleepTime}->{ $task_id };
    }

  delete $heap->{task}->{$task_id};
  delete $heap->{directory}->{$task_id};
}

sub got_sigchld {
  return 0;
}


1;
