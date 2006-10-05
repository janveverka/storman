use strict;
use warnings;
package T0::Merge::FastMerge;
use POE qw( Wheel::Run Filter::Line );

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

sub new
{
  my ($class, $hash_ref) = @_;
  my $self = {};
  bless($self, $class);

  POE::Session->create(
		       inline_states => {
					 _start => \&start_tasks,
					 start_wheel => \&start_wheel,
					 monitor_task => \&monitor_task,
					 cleanup_task => \&cleanup_task,
					 got_task_stdout => \&got_task_stdout,
					 got_task_stderr => \&got_task_stderr,
					 got_task_close	=> \&got_task_close,
					 got_sigchld => \&got_sigchld,
					},
		       args => [ $hash_ref, $self ],
		      );

  return $self;
}

sub start_tasks {
  my ( $kernel, $heap, $hash_ref, $self ) = @_[ KERNEL, HEAP, ARG0, ARG1 ];

  # remember hash reference
  $heap->{inputhash} = $hash_ref;

  # remember refercne to myself
  $heap->{Self} = $self;

  # remember SvcClass
  $heap->{svcclass} = $hash_ref->{svcclass};

  # put callback session and method on heap
  $heap->{session} = $hash_ref->{session};
  $heap->{callback} = $hash_ref->{callback};

  # put job report and output catalog name on heap
  $heap->{jobreport} = $hash_ref->{jobreport};
  $heap->{outputcatalog} = $hash_ref->{outputcatalog};

  # before spawning wheel, register signal handler
  $kernel->sig( CHLD => "got_sigchld" );

  # create EdmFastMerge input and output
  $heap->{arguments} = [];
  foreach my $file ( @{ $hash_ref->{sources} } )
    {
      push(@{$heap->{arguments}}, '--in ' . $file);
    }
  push(@{$heap->{arguments}}, '--out ' . $hash_ref->{target});
  push(@{$heap->{arguments}}, '--jobreport ' . $hash_ref->{jobreport});
  push(@{$heap->{arguments}}, '--writecatalog ' . $hash_ref->{outputcatalog});

  # configure number of retries
  if ( exists $hash_ref->{retries} )
    {
      $heap->{retries} = $hash_ref->{retries};
    }
  else # set to zero, makes the followup code a little easier
    {
      $heap->{retries} = 0;
    }

  # configure retry delay
  if ( exists $hash_ref->{retry_backoff} )
    {
      $heap->{retry_backoff} = $hash_ref->{retry_backoff};
    }

  # configure timeout;
  if ( exists $hash_ref->{timeout} )
    {
      $heap->{timeout} = $hash_ref->{timeout}
    }

  $kernel->yield('start_wheel');
}

sub start_wheel {
  my ( $kernel, $heap ) = @_[ KERNEL, HEAP ];

  $ENV{STAGE_SVCCLASS} = $heap->{svcclass} if ( defined $heap->{svcclass} );

  my $task = POE::Wheel::Run->new(
				  Program => "EdmFastMerge",
				  ProgramArgs => $heap->{arguments},
				  StdoutFilter => POE::Filter::Line->new(),
				  StdoutEvent  => "got_task_stdout",
				  StderrEvent  => "got_task_stderr",
				  CloseEvent   => "got_task_close",
				 );

  $heap->{task}->{ $task->ID } = $task;
  $heap->{pid}->{ $task->PID } = $task->ID;

  # spawn monitoring thread
  if ( exists $heap->{timeout} )
    {
      $kernel->delay_set('monitor_task',$heap->{timeout},($task->ID,0));
    }
}

sub monitor_task {
  my ( $kernel, $heap, $task_id, $force_kill ) = @_[ KERNEL, HEAP, ARG0, ARG1 ];

  if ( exists $heap->{task}->{ $task_id } )
    {
      if ( $force_kill == 0 )
	{
#	  print "Task $task_id still active, kill it\n";

	  $heap->{task}->{ $task_id }->kill();

	  # 10 seconds should be enough for task to exit
	  $kernel->delay_set('monitor_task',10,($task_id,1));
	}
      else
	{
#	  print "Task $task_id still active, kill it by force\n";

	  $heap->{task}->{ $task_id }->kill(9);

	  # cleanup task if it doesn't exit after another 10 seconds
	  $kernel->delay_set('cleanup_task',10,($task_id,-1));
	}
    }
}

sub got_task_stdout {
  my ( $kernel, $heap, $stdout, $task_id ) = @_[ KERNEL, HEAP, ARG0, ARG1 ];
  print "FASTMERGE STDOUT: $stdout\n";
}

sub got_task_stderr {
  my ( $stderr, $task_id ) = @_[ ARG0, ARG1 ];
  print "FASTMERGE STDERR: $stderr\n";
}

sub got_task_close {
  my ( $kernel, $heap, $task_id ) = @_[ KERNEL, HEAP, ARG0 ];
}

sub got_sigchld {
  my ( $kernel, $heap, $child_pid, $status ) = @_[ KERNEL, HEAP, ARG1, ARG2 ];

  if ( exists $heap->{pid}->{$child_pid} )
    {
      my $task_id = $heap->{pid}->{$child_pid};

      if ( exists $heap->{task}->{ $task_id } )
	{
	  $kernel->yield('cleanup_task',($task_id,$status));
	}
    }

  return 0;
}

sub cleanup_task {
  my ( $kernel, $heap, $task_id, $status ) = @_[ KERNEL, HEAP, ARG0, ARG1 ];

  if ( exists $heap->{task}->{ $task_id } )
    {
      delete $heap->{task}->{$task_id};
      delete $heap->{file}->{$task_id};

      # update status in caller hash
      $heap->{inputhash}->{status} = $status;

      if ( $status != 0 )
	{
	  $heap->{Self}->Debug("FastMerge failed with status $status\n");

	  if ( $heap->{retries} > 0 )
	    {
	      $heap->{Self}->Debug("Retry count at " . $heap->{retries} . " , retrying\n");

	      $heap->{retries}--;

	      if ( exists $heap->{retry_backoff} )
		{
		  $kernel->delay_set('start_wheel',$heap->{retry_backoff});
		}
	      else
		{
		  $kernel->yield('start_wheel');
		}
	    }
	  else
	    {
	      $heap->{Self}->Debug("Retry count at " . $heap->{retries} . " , abandoning\n");
	      $kernel->post( $heap->{session}, $heap->{callback}, $heap->{inputhash} );
	    }
	}
      else
	{
	  $kernel->post( $heap->{session}, $heap->{callback}, $heap->{inputhash} );
	}
    }
}

1;
