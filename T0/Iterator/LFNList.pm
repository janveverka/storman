use strict;
use warnings;
package T0::Iterator::LFNList;
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

  my $task = POE::Wheel::Run->new(
				  Program => [ "cat", $hash_ref->{LFNList} ],
				  StdoutFilter => POE::Filter::Line->new(),
				  StdoutEvent  => "got_task_stdout",
				  StderrEvent  => "got_task_stderr",
				  CloseEvent   => "got_task_close",
				 );

  $heap->{task}->{ $task->ID } = $task;
}

sub got_task_stdout {
  my ( $kernel, $heap, $stdout, $task_id ) = @_[ KERNEL, HEAP, ARG0, ARG1 ];

  chomp($stdout);

  # every line represents a LFN, convert here to PFN
  my $filename = '/castor/cern.ch/cms' . $stdout;

  # get file stats
  my @stats = qx {rfstat $filename 2> /dev/null};

  if ( scalar @stats == 0 )
    {
      # file doesn't exist
      return;
    }
  else
    {
      my $size = undef;
      my $secondsSinceEpoch = undef;

      foreach my $stat ( @stats )
	{
	  if ( $stat =~ /^Last modify/)
	    {
	      chomp($stat);

	      my @temp = split (" : ",$stat);
	      $secondsSinceEpoch = UnixDate( ParseDate($temp[1]), "%s" );
	    }
	  if ( $stat =~ /^Size/)
	    {
	      chomp($stat);

	      my @temp = split (" : ",$stat);
	      $size = $temp[1];
	    }
	}

      if ( defined $size and defined $secondsSinceEpoch )
	{
	  $heap->{Files}->{Status}->{$filename} = 0;
	  if ( exists $heap->{Files}->{Size} ) { $heap->{Files}->{Size}->{$filename} = $size; }
	  if ( exists $heap->{Files}->{Date} ) { $heap->{Files}->{Date}->{$filename} = $secondsSinceEpoch; }
	}
    }

  return;
}

sub got_task_stderr {
    my $stderr = $_[ARG0];
    print "CAT STDERR: $stderr\n";
}

sub got_task_close {
  my ( $kernel, $heap, $task_id ) = @_[ KERNEL, HEAP, ARG0 ];

  delete $heap->{task}->{$task_id};
}

sub got_sigchld {
  return 0;
}


1;
