########################################################################################################
#
# Exec rfcp command with all the given files
#
########################################################################################################
#
# This module has to receive (in the call to new) a hash containing:
# -svcclass =>
# -session => The session which we will come back to after rfstat.
# -callback => The function of the session we will call back to. We will send the input hash as argument.
# -files => An array containing the information we need to execute rfcp.
#
# Optionally:
# -retries => The number of retries we will do before giving up.
# -retry_backoff => The number of seconds we will wait before the next try.
# -timeout => Time we will wait before killing the wheel executing the command.
# -delete_bad_files => If is set (=1) we will delete bad files created after a unsuccessfull rfcp.
#
#
# Each element of the files array has to contain:
# -source => Path of the source file.
# -target => Path of the target dir + filename.
#
#
# After finishing the rfcp commands the funcion will call back the specified funcion
# and will add to each element of the files array:
# -status => It will be 0 if everything went fine or !=0 if there was something wrong.
#
########################################################################################################


use strict;
use warnings;
package T0::Castor::Rfcp;
use POE qw( Wheel::Run Filter::Line );
use File::Basename;
use T0::Castor::RfstatHelper;

our (@ISA, @EXPORT, @EXPORT_OK, %EXPORT_TAGS, $VERSION);

use Carp;
$VERSION = 1.00;
@ISA = qw/ Exporter /;

our $hdr = __PACKAGE__ . ':: ';
sub Croak   { croak $hdr,@_; }
sub Carp    { carp  $hdr,@_; }
sub Verbose { T0::Util::Verbose( (shift)->{Verbose}, ("RFCP:\t", @_) ); }
sub Debug   { T0::Util::Debug(   (shift)->{Debug},   ("RFCP:\t", @_) ); }
sub Quiet   { T0::Util::Quiet(   (shift)->{Quiet},   ("RFCP:\t", @_) ); }

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
					 rfcp_exit_handler => \&rfcp_exit_handler,
					 check_target_exists => \&check_target_exists,
					 rfcp_retry_handler => \&rfcp_retry_handler,
					 wheel_cleanup => \&wheel_cleanup,
					 got_task_stdout => \&got_task_stdout,
					 got_task_stderr => \&got_task_stderr,
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

  # remember reference to myself
  $heap->{Self} = $self;

  # remember SvcClass
  $heap->{svcclass} = $hash_ref->{svcclass};

  # remeber (delete bad files) option
  $heap->{delete_bad_files} = $hash_ref->{delete_bad_files};

  # put callback session and method on heap
  $heap->{session} = $hash_ref->{session};
  $heap->{callback} = $hash_ref->{callback};

  # keep count on outstanding rfcp wheels
  $heap->{wheel_count} = 0;

  # store output of child processes
  $heap->{output} = [];

  # before spawning wheels, register signal handler
  $kernel->sig( CHLD => "got_sigchld" );

  if ( defined $heap->{svcclass} )
    {
      $ENV{STAGE_SVCCLASS} = $heap->{svcclass};
    }
  else
    {
      $heap->{Self}->Quiet("SvcClass not set, use t0input!\n");
      $ENV{STAGE_SVCCLASS} = 't0input';
    }

  # spawn wheels
  foreach my $file ( @{ $hash_ref->{files} } )
    {
      # hash to be passed to wheel
      my %filehash = (
		      original => $file,
		      source => $file->{source},
		      target => $file->{target},
		     );

      # configure number of retries
      my $retries = undef;
      if ( exists $file->{retries} )
	{
	  $retries = $file->{retries};
	}
      elsif ( exists $hash_ref->{retries} )
	{
	  $retries = $hash_ref->{retries};
	}
      else # set to zero, makes the followup code a little easier
	{
	  $retries = 0;
	}

      $filehash{retries} = $retries;

      # configure retry delay
      my $retry_backoff = undef;
      if ( exists $file->{rety_backoff} )
	{
	  $retry_backoff = $file->{retry_backoff};
	}
      elsif ( exists $hash_ref->{retry_backoff} )
	{
	  $retry_backoff = $hash_ref->{retry_backoff};
	}
      if ( defined $retry_backoff )
	{
	  $filehash{retry_backoff} = $retry_backoff;
	}

      # configure timeout;
      my $timeout = undef;
      if ( exists $file->{timeout} )
	{
	  $timeout = $file->{timeout}
	}
      elsif ( exists $hash_ref->{timeout} )
	{
	  $timeout = $hash_ref->{timeout}
	}
      if ( defined $timeout )
	{
	  $filehash{timeout} = $timeout;
	}

      # this variable will avoid multiple checks about source file missing.
      $filehash{checked_source} = 0;

      # keep track of what target directories have been created
      $heap->{created_target_dir} = ();

      $heap->{wheel_count}++;
      $kernel->yield('start_wheel',(\%filehash));
    }
}

sub start_wheel {
  my ( $kernel, $heap, $file ) = @_[ KERNEL, HEAP, ARG0 ];

  my $program = 'rfcp';
  my @arguments = ( $file->{source}, $file->{target} );

  $heap->{Self}->Quiet("Start copy from $file->{source} to $file->{target}\n");

  #$ENV{STAGER_TRACE} = 3;
  #$ENV{RFIO_TRACE} = 3;

  my $task = POE::Wheel::Run->new(
				  Program => $program,
				  ProgramArgs => \@arguments,
				  StdoutFilter => POE::Filter::Line->new(),
				  StdoutEvent  => "got_task_stdout",
				  StderrEvent  => "got_task_stderr",
				 );

  $heap->{task}->{ $task->ID } = $task;
  $heap->{file}->{ $task->ID } = $file;

  $heap->{pid}->{ $task->PID } = $task->ID;

  # spawn monitoring thread
  if ( exists $file->{timeout} )
    {
      $file->{alarm_id} = $kernel->delay_set('monitor_task',$file->{timeout},($task->ID,0));
    }
}

sub monitor_task {
  my ( $kernel, $heap, $task_id, $force_kill ) = @_[ KERNEL, HEAP, ARG0, ARG1 ];

  if ( exists $heap->{task}->{ $task_id } )
    {
      my $file = $heap->{file}->{$task_id};

      delete $file->{alarm_id};

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
	  $kernel->delay_set('rfcp_exit_handler',10,($task_id,-1));
	}
    }
}

sub got_task_stdout {
  my ( $kernel, $heap, $stdout, $task_id ) = @_[ KERNEL, HEAP, ARG0, ARG1 ];
#  print "RFCP STDOUT: $stdout\n";

#  push( @{ $heap->{output} }, "RFCP STDOUT: " . $stdout . "\n");

  my $file = $heap->{file}->{$task_id};
  my $test = open(LOGFILE, '>>' . basename($file->{source}) . '.log');
  print LOGFILE "$stdout\n";
  close(LOGFILE);
}

sub got_task_stderr {
  my ( $kernel, $heap, $stderr, $task_id ) = @_[ KERNEL, HEAP, ARG0, ARG1 ];
#  print "RFCP STDERR: $stderr\n";

#  push( @{ $heap->{output} }, "RFCP STDERR: " . $stderr);

  my $file = $heap->{file}->{$task_id};
  open(LOGFILE, '>>' . basename($file->{source}) . '.log');
  print LOGFILE "$stderr\n";
  close(LOGFILE);
}

sub got_sigchld {
  my ( $kernel, $heap, $child_pid, $status ) = @_[ KERNEL, HEAP, ARG1, ARG2 ];

  if ( exists $heap->{pid}->{$child_pid} )
    {
      my $task_id = $heap->{pid}->{$child_pid};

      delete $heap->{pid}->{$child_pid};

      if ( exists $heap->{task}->{ $task_id } )
	{
	  $kernel->yield('rfcp_exit_handler',($task_id,$status));
	}
    }
}


# This process will try to recover from any error.
# This one check if there has been any problem and if so check if the source file exist.
sub rfcp_exit_handler {
  my ( $kernel, $heap, $session, $task_id, $status ) = @_[ KERNEL, HEAP, SESSION, ARG0, ARG1 ];

  if ( exists $heap->{task}->{ $task_id } )
    {
      my $file = $heap->{file}->{$task_id};

      if ( exists $file->{alarm_id} )
	{
	  $kernel->alarm_remove( $file->{alarm_id} );
	}

      # update status in caller hash
      $file->{original}->{status} = $status;

      # Something went wrong
      if ( $status != 0 )
	{
	  $heap->{Self}->Quiet("Rfcp of " . $file->{source} . " failed with status $status\n");

	  # Check if the source file exists just the first time.
	  if( !$file->{checked_source} )
	    {

	      $heap->{Self}->Quiet("Checking if file " . $file->{source} . " exists\n");
	      my $file_status = T0::Castor::RfstatHelper::checkFileExists( $file->{source} );

	      $file->{checked_source} = 1;

	      # Rfstat failed. Source doesn't exist
	      # If the source file doesn't exist we have nothing else to do.
	      if ( $file_status != 0 )
		{
		  $heap->{Self}->Quiet("Source file " . $file->{source} . " does not exist\n");
		  $kernel->yield('wheel_cleanup', ($task_id,$status));
		}
	      # Source exists
	      # If it exists continue with the cleanup.
	      else
		{
		  $heap->{Self}->Quiet("Source file " . $file->{source} . " exists\n");
		  $kernel->yield('check_target_exists', ($task_id,$status));
		}
	    }
	  else
	    {
	      $kernel->yield('check_target_exists',($task_id,$status));
	    }
	}
      # rfcp succeeded, just do some cleanup
      else
	{
	  $heap->{Self}->Quiet("$file->{source} successfully copied\n");
	  $kernel->yield('wheel_cleanup',($task_id,$status));
	}
    }
}


# Check for existence of directory (if status is 256 or 512)
sub check_target_exists {
  my ( $kernel, $heap, $session, $task_id, $status ) = @_[ KERNEL, HEAP, SESSION, ARG0, ARG1 ];

  my $file = $heap->{file}->{$task_id};

  if ( ($status == 256 || $status == 512) )
    {
      my $targetdir = dirname( $file->{target} );

      # check if directory was already created by other file copy error handler
      if ( exists $heap->{created_target_dir}->{$targetdir} )
	{
	  # assume the directory has been created and retry
	  $kernel->yield('rfcp_retry_handler', ($task_id,$status,1,1));
	}
      else
	{
	  $heap->{Self}->Quiet("Checking if directory $targetdir exists\n");
	  my $dir_status = T0::Castor::RfstatHelper::checkDirExists( $targetdir );

	  # The target doesn't exists. Create the directory
	  if ( $dir_status == 1 )
	    {
	      $heap->{Self}->Quiet("Creating directory $targetdir\n");
	      my @args = ("rfmkdir", "-p", $targetdir);
	      if ( system(@args) == 0 )
		{
		  $heap->{created_target_dir}->{$targetdir} = 1;
		  $kernel->yield('rfcp_retry_handler', ($task_id,$status,1,0));
		}
	      else
		{
		  # something went wrong in directory creation
		  # normal retry, hope works better next round
		  $heap->{Self}->Quiet("Could not create directory $targetdir\n");
		  $kernel->yield('rfcp_retry_handler', ($task_id,$status,0,0));
		}
	    }
	  elsif ($dir_status == 2)
	    {
	      # The targetdir is not a dir. Stop the iteration
	      $heap->{Self}->Quiet("$targetdir is not a directory\n");
	      $kernel->yield('wheel_cleanup', ($task_id,$status));
	    }
	  # Target exists and it is a directory
	  elsif ($dir_status == 0)
	    {
	      $heap->{Self}->Quiet("Directory $targetdir exists\n");
	      $kernel->yield('rfcp_retry_handler', ($task_id,$status,0,1));
	    }
	}
    }
  # no problems with target, regular retry
  else
    {
      $kernel->yield('rfcp_retry_handler', ($task_id,$status,0,1));
    }
}


# Remove target file if it exists only if I didn't create the target
# directory in the previous step
sub rfcp_retry_handler {
  my ( $kernel, $heap, $session, $task_id, $status, $createdTargetDir, $deleteTargetFile ) = @_[ KERNEL, HEAP, SESSION, ARG0, ARG1, ARG2, ARG3 ];

  my $file = $heap->{file}->{$task_id};

  if ( defined($heap->{delete_bad_files}) && $heap->{delete_bad_files} == 1
       && $deleteTargetFile == 1 )
    {
      $heap->{Self}->Quiet("Deleting file before retrying\n");

      if ( $file->{target} =~ m/^\/castor/ )
	{
	  qx {stager_rm -M $file->{target} 2> /dev/null};
	  qx {nsrm $file->{target} 2> /dev/null};
	}
      else
	{
	  qx {rfrm $file->{target} 2> /dev/null};
	}
    }

  # After creating the dir we retry without waiting and without decreasing retries
  if ( $createdTargetDir == 1 )
    {
      $heap->{wheel_count}++;
      $kernel->yield('start_wheel',($file));
    }
  # Retrying
  elsif ( $file->{retries} > 0 )
    {
      $heap->{Self}->Quiet("Retry count at " . $file->{retries} . " , retrying\n");
      $file->{retries}--;

      if ( exists $file->{retry_backoff} )
	{
	  $heap->{wheel_count}++;
	  $kernel->delay_set('start_wheel',$file->{retry_backoff},($file));
	}
      else
	{
	  $heap->{wheel_count}++;
	  $kernel->yield('start_wheel',($file));
	}
    }
  # No more retries
  else
    {
      $heap->{Self}->Quiet("Retry count at " . $file->{retries} . " , abandoning\n");
    }
  $kernel->yield('wheel_cleanup', ($task_id,$status));
}

# Free space in memory assigned to the wheel.
sub wheel_cleanup {
  my ( $kernel, $heap, $task_id ) = @_[ KERNEL, HEAP, ARG0 ];

  my $file = $heap->{file}->{$task_id};

  $heap->{wheel_count}--;

  # Clean up all the session
  if ( $heap->{wheel_count} == 0 )
    {
      $kernel->post( $heap->{session}, $heap->{callback}, $heap->{inputhash} );

      delete $heap->{inputhash};
      delete $heap->{Self};
      delete $heap->{svcclass};
      delete $heap->{session};
      delete $heap->{callback};
      delete $heap->{wheel_count};
      delete $heap->{output};

      delete $heap->{task};
      delete $heap->{file};
      delete $heap->{pid};
    }
  # Clean up this wheel
  else
    {
      delete $heap->{task}->{$task_id};
      delete $heap->{file}->{$task_id};
    }
}


1;
