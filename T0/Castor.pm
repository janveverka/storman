package T0::Castor;
use strict;
use warnings;

########################################################################################################
#
# Copy all the given files
#
########################################################################################################
#
# This module has to receive (in the call to new) a hash containing:
# id => some unique ID to keep track of the copy
# svcclass =>
# session => The session which we will come back to after successful copy.
# callback => The function of the session we will call back to. We will send the input hash as argument.
# files => An array containing the information we need to copy the files.
#
# Optionally:
# retries => The number of retries we will do before giving up.
# retry_backoff => The number of seconds we will wait before the next try.
# timeout => Time we will wait before killing the wheel executing the command.
# delete_bad_files => If set, we will delete bad files created after an unsuccessfull copy.
#
#
# Each element of the files array has to contain:
# source => Path of the source file.
# target => Path of the target dir + filename.
#
#
# After finishing the copy commands the function will call back the specified
# function and will add to each element of the files array:
# status => It will be 0 if everything went fine or !=0 if there was something wrong.
#
########################################################################################################

use POE qw( Wheel::Run Filter::Line );
use File::Basename;
use File::Path qw( mkpath );
use T0::Castor::RfstatHelper;
use T0::Util;
use POSIX qw( strftime );
use Carp;
use Exporter qw( import );

our @EXPORT_OK = qw($castor_alias);
our @EXPORT    = @EXPORT_OK;

our $castor_alias = 'Castor';    # Session name

our $VERSION = 1.00;
our @ISA     = qw/ Exporter /;

our $hdr = __PACKAGE__ . ':: ';
sub Croak { croak $hdr, @_; }
sub Carp  { carp $hdr,  @_; }
sub Verbose { T0::Util::Verbose( (shift)->{Verbose}, "RFCP:\t", @_ ); }
sub Debug { T0::Util::Debug( (shift)->{Debug}, "RFCP:\t", @_ ); }
sub Quiet { T0::Util::Quiet( (shift)->{Quiet}, "RFCP:\t", @_ ); }

#
# check if rfcp version supports checksum
# (default is off)
#
my $use_checksum = 0;

my $output = qx{castor -v 2>/dev/null};

# This could be simplified using version.pm, but still in 5.8.8, so...
if ($output) {
    my ( $major, $minor, $revision, $qualifier ) = split( /[.-]/, $output );
    if (   ( $major > 2 )
        || ( $major == 2 && $minor > 1 )
        || ( $major == 2 && $minor == 1 && $revision > 8 )
        || ( $major == 2 && $minor == 1 && $revision == 8 && $qualifier >= 12 )
      )
    {
        $use_checksum = 1;
    }
}

sub new {
    my ( $class, $hash_ref ) = @_;

    if ( $poe_kernel->alias_resolve($castor_alias) ) {  # Session already exists
        $poe_kernel->post( $castor_alias => 'add_copy' => $hash_ref );
    }
    else {                                              # Create session
        my $self = bless( {}, $class );
        POE::Session->create(
            inline_states => {
                _start              => \&start,
                add_copy            => \&add_copy,
                add_task            => \&add_task,
                next_task           => \&next_task,
                start_wheel         => \&start_wheel,
                monitor_task        => \&monitor_task,
                cleanup_task        => \&cleanup_task,
                remove_task         => \&remove_task,
                exit_handler        => \&exit_handler,
                check_target_exists => \&check_target_exists,
                retry_handler       => \&retry_handler,
                got_task_stdout     => \&got_task_stdout,
                got_task_stderr     => \&got_task_stderr,
                got_task_output     => \&got_task_output,
                got_sigchld         => \&got_sigchld,
                rfcp_exit_handler   => \&rfcp_exit_handler,
                rfcp_retry_handler  => \&rfcp_retry_handler,
                terminate           => \&terminate,
                _stop               => \&terminate,
                _default            => \&handle_default,
            },
            args => [ $hash_ref, $self ],
        );
    }

    return;    # No need to return anything
}

sub start {
    my ( $kernel, $heap, $hash_ref, $self ) = @_[ KERNEL, HEAP, ARG0, ARG1 ];
    $kernel->alias_set($castor_alias);

    # remember reference to myself
    $heap->{Self} = $self;

    # before spawning wheels, register signal handler
    $kernel->sig( CHLD => "got_sigchld" );

    $kernel->yield( 'add_copy' => $hash_ref );
}

# Do a clean shutdown, so stop starting wheels
sub terminate {
    my ( $kernel, $heap ) = @_[ KERNEL, HEAP ];
    $heap->{Shutdown} = 1;
}

# Splits input copies into individual tasks, one per file
sub add_copy {
    my ( $kernel, $heap, $hash_ref ) = @_[ KERNEL, HEAP, ARG0 ];

    # Update the maximum number of workers
    $heap->{MaxWorkers} = $hash_ref->{max_workers}
      if exists $hash_ref->{max_workers};
    foreach my $file ( @{ $hash_ref->{files} } ) {
        my %filehash = (    # hash to be passed to wheel
            inputhash      => $hash_ref,    # remember hash reference
            checked_source => 0,
            %$file,
        );

        # ensure default values for some parameters
        for my $param (qw( id retries retry_backoff timeout )) {
            $filehash{$param} =
                exists $hash_ref->{$param}
              ? ref( $hash_ref->{$param} )
                  ? ref( $hash_ref->{$param} ) eq 'ARRAY'
                      ? [ @{ $hash_ref->{$param} } ]
                      : ref( $hash_ref->{$param} ) eq 'HASH'
                      ? { %{ $hash_ref->{$param} } }
                      : $hash_ref->{$param}
                  : $hash_ref->{$param}
              : 0
              unless defined $filehash{$param};
        }

        # To print out a unified header
        $filehash{header} = "[$filehash{id}] ";

        $kernel->yield( 'add_task' => \%filehash );
    }
}

# Pushes what needs to be done on a queue
sub add_task {
    my ( $kernel, $heap, $arg ) = @_[ KERNEL, HEAP, ARG0 ];

    # see if with or without checksum check
    if (   $use_checksum
        && $arg->{checksum}
        && $arg->{target} =~ m/^\/castor/ )
    {
        $arg->{Program}     = \&rfcp_copy_with_checksum_and_check;
        $arg->{ProgramArgs} = [$arg];
    }
    else {
        $arg->{Program} = "rfcp";
        $arg->{ProgramArgs} = [ $arg->{source}, $arg->{target} ];
    }

    push @{ $heap->{task_list} }, $arg;
    $kernel->yield('next_task');
}

# Remove a task, when the file has already been queued
sub remove_task {
    my ( $kernel, $heap, $id ) = @_[ KERNEL, HEAP, ARG0 ];
    $heap->{task_list} = [ grep { $_->{id} != $id } @{ $heap->{task_list} } ];
}

# Creates a new wheel to start the copy
sub next_task {
    my ( $kernel, $heap ) = @_[ KERNEL, HEAP ];
    my $maxworkers = $heap->{MaxWorkers} || 3;
    if ( keys( %{ $heap->{task} } ) < $maxworkers ) {
        my $arg = shift @{ $heap->{task_list} };
        return unless defined $arg;    # Nothing to do
        $kernel->yield( 'start_wheel' => $arg );
    }
}

# Actually do the work by forking a wheel
sub start_wheel {
    my ( $kernel, $heap, $arg, $task_id ) = @_[ KERNEL, HEAP, ARG0, ARG1 ];

    if ( $heap->{Shutdown} ) {
        if ( my $count = keys %{ $heap->{task} } ) {
            $heap->{Self}->Quiet( "Shutting down, but still $count task"
                  . ( $count > 1 ? 's' : '' )
                  . " left.\n" );
        }
        else {
            $kernel->yield("shutdown");
        }
    }
    delete $heap->{task}->{$task_id} if $task_id;    # Remove old ID upon retry

    my $task = POE::Wheel::Run->new(
        Program      => $arg->{Program},
        ProgramArgs  => $arg->{ProgramArgs},
        StdoutFilter => POE::Filter::Line->new(),
        StderrFilter => POE::Filter::Line->new(),
        StdoutEvent  => 'got_task_stdout',
        StderrEvent  => 'got_task_stderr',
    );
    $kernel->sig_child( $task->PID, "got_sigchld" );
    $heap->{arg}->{ $task->ID }  = $arg;
    $heap->{pid}->{ $task->PID } = $task->ID;
    $heap->{task}->{ $task->ID } = $task;
    my $oldheader = $arg->{header};

    my $header =
      '[' . $arg->{id} . '-' .
      keys( %{ $heap->{task} } ) . '-' . $task->ID . '-' . $task->PID . ']';
    $arg->{header} = $header;
    if ($task_id) {
        $heap->{Self}
          ->Quiet("$header Restarted copy $arg->{id} (was: $oldheader)\n");
    }
    else {
        $heap->{Self}->Quiet("$header Started copy $arg->{id}\n");
    }
    $heap->{Self}->Quiet("$header Source: $arg->{source}\n");
    $heap->{Self}->Quiet("$header Target: $arg->{target}\n");
    $heap->{Self}->Quiet("$header Size: $arg->{size}\n");
    $heap->{Self}->Quiet("$header Adler32 checksum: $arg->{checksum}\n")
      if $arg->{checksum};

    # spawn monitoring thread
    if ( $arg->{timeout} ) {
        $arg->{alarm_id} = $kernel->delay_set(
            'monitor_task' => $arg->{timeout},
            $task->ID, 0
        );
    }

    # See if there is something more to be processed
    $kernel->yield('next_task');
}

# Ensure that after the timeout expires, task is finished, otherwise kill it
sub monitor_task {
    my ( $kernel, $heap, $task_id, $force_kill ) =
      @_[ KERNEL, HEAP, ARG0, ARG1 ];

    if ( exists $heap->{task}->{$task_id} ) {
        my $task   = $heap->{task}->{$task_id};
        my $arg    = $heap->{arg}->{$task_id};
        my $header = $arg->{header};

        delete $arg->{alarm_id};

        if ($force_kill) {
            $heap->{Self}->Quiet(
                "$header Rfcp of $arg->{source} timed out: force killing!\n");
            $task->kill(9);

            # cleanup task if it doesn't exit after another 10 seconds
            $kernel->delay_set( 'rfcp_exit_handler' => 10, $task_id, -1 );
        }
        else {
            $heap->{Self}
              ->Quiet("$header Rfcp of $arg->{source} timed out: killing!\n");
            $task->kill();

            # 10 seconds should be enough for task to exit
            $arg->{alarm_id} =
              $kernel->delay_set( 'monitor_task' => 10, $task_id, 1 );

        }
    }
}

# Copy emitted a line on STDOUT
sub got_task_stdout {
    my ( $kernel, $session, $stdout, $task_id ) =
      @_[ KERNEL, SESSION, ARG0, ARG1 ];
    $kernel->call( $session, got_task_output => $stdout, $task_id, 'STDOUT' );
}

# Copy emitted a line on STDERR
sub got_task_stderr {
    my ( $kernel, $session, $stderr, $task_id ) =
      @_[ KERNEL, SESSION, ARG0, ARG1 ];
    $kernel->call( $session, got_task_output => $stderr, $task_id, 'ERROR' );
}

# Write lines emitted by the copy to a logfile
sub got_task_output {
    my ( $kernel, $heap, $line, $task_id, $kind ) =
      @_[ KERNEL, HEAP, ARG0 .. ARG2 ];

    my $arg = $heap->{arg}->{$task_id};
    my $logdir = $arg->{logdir} || '.';
    if ( $logdir && !-d $logdir ) {
        mkpath $logdir;
    }
    my $logfilename = $logdir . '/' . basename( $arg->{source} ) . '.log';
    open( my $logfile, '>>', $logfilename )
      or die "Cannot open logfile $logfilename: $!";
    print $logfile strftime( "%Y-%m-%d %H:%M:%S", localtime time )
      . " $arg->{header} $kind: $line\n";
    close($logfile);
}

# Copy has finished, child exited
sub got_sigchld {
    my ( $kernel, $heap, $child_pid, $status ) = @_[ KERNEL, HEAP, ARG1, ARG2 ];

    if ( my $task_id = delete $heap->{pid}->{$child_pid} ) {
        if ( exists $heap->{task}->{$task_id} ) {
            $kernel->yield( 'rfcp_exit_handler', $task_id, $status >> 8 );
        }
        $kernel->sig_handled;
    }
    else {
        $heap->{Self}->Verbose("Got signal SIGCHLD for unknown PID $child_pid");
    }
}

# This process will try to recover from any error.
# This one check if there has been any problem and if so check if the source file exist.
sub rfcp_exit_handler {
    my ( $kernel, $heap, $session, $task_id, $status ) =
      @_[ KERNEL, HEAP, SESSION, ARG0, ARG1 ];

    my $arg    = $heap->{arg}->{$task_id};
    my $header = $arg->{header};

    if ( exists $arg->{alarm_id} ) {
        $kernel->alarm_remove( delete $arg->{alarm_id} );
    }

    if ( $status != 0 ) {    # Something went wrong
        $heap->{Self}->Quiet(
            "$header Rfcp of $arg->{source} failed with status $status\n");

        # Check if the source file exists just the first time.
        if ( !$arg->{checked_source} ) {
            $heap->{Self}
              ->Quiet("$header Checking if file $arg->{source} exists\n");
            my $file_status =
              T0::Castor::RfstatHelper::checkFileExists( $arg->{source} );

            $arg->{checked_source} = 1;

            # Rfstat failed. Source doesn't exist
            # If the source file doesn't exist we have nothing else to do.
            if ( $file_status != 0 ) {
                $heap->{Self}->Quiet(
                    "$header Source file $arg->{source} does not exist\n");
                $kernel->yield( 'cleanup_task' => $task_id );
            }

            # Source exists
            # If it exists continue with the cleanup.
            else {
                $heap->{Self}
                  ->Quiet("$header Source file $arg->{source} exists\n");
                $kernel->yield( 'check_target_exists', $task_id, $status );
            }
        }
        else {
            $kernel->yield( 'check_target_exists', $task_id, $status );
        }
    }
    else {    # rfcp succeeded, cleanup and call back the main session
        $heap->{Self}->Quiet("$header $arg->{source} successfully copied\n");
        $kernel->yield( 'cleanup_task' => $task_id );
    }
}

# When a task is done (success or failure), cleanup and notify the Worker
sub cleanup_task {
    my ( $kernel, $heap, $session, $task_id, $status ) =
      @_[ KERNEL, HEAP, SESSION, ARG0, ARG1 ];

    # Cleanup references to the task which just finished
    my $arg = delete $heap->{arg}->{$task_id};
    delete $heap->{task}->{$task_id};

    # update status in caller hash
    $arg->{inputhash}->{status} = $status;

    # Notify the Worker of the result
    $kernel->post(
        $arg->{inputhash}->{session},
        $arg->{inputhash}->{callback},
        $arg->{inputhash}
    );

    # See if there is something more to be processed
    $kernel->yield('next_task');
}

sub rfcp_copy_with_checksum_and_check {
    my $arg = shift;
    my ( $source, $target, $checksum, $size ) =
      @$arg{qw(source target checksum size)};
    if ( exists $arg->{inputhash}->{svcclass} ) {
        $ENV{STAGE_SVCCLASS} = $arg->{inputhash}->{svcclass};
    }
    else {
        print STDERR "SvcClass not set, use t0input!\n";
        $ENV{STAGE_SVCCLASS} = 't0input';
    }
    exit_if_error( system => nstouch => $target );
    exit_if_error(
        system => nssetchecksum => -n => adler32 => -k => $checksum =>
          $target );
    exit_if_error( system => rfcp => $source => $target );
    my $check = exit_if_error( qx => nsls => -l => '--checksum' => $target );
    my ( $cSize, $cChecksum ) = $check =~ /^
                          \S+\s+      # rights
                          \d+\s+      # refcount
                          \S+\s+      # owner
                          \S+\s+      # group
                          (\d+)\s+    # size     (captured)
                          \S+\s+      # month
                          \d+\s+      # day of the month
                          \S+\s+      # time
                          AD\s+       # Adler32 checksum
                          (\w+)\s+    # checksum (captured)
                          \S+         # filename
                          $/x;

    if ( defined $cSize && defined $cChecksum ) {
        if ( $cSize != $size ) {
            die "size $cSize != $size!";
        }
        elsif ( $cChecksum ne $checksum ) {
            die "checksum $cChecksum != $checksum!";
        }
    }
    else {
        die "$check does not match pattern. Retrying!";
    }
    exit;
}

sub exit_if_error {
    return unless @_;
    my $command  = shift;
    my $exitcode = -1;
    my $output   = '';
    if ( $command eq 'system' ) {
        $exitcode = system(@_);
    }
    elsif ( $command eq 'qx' ) {
        chomp( $output = qx{@_} );
        $exitcode = $?;
    }
    else {
        die "No idea what to do with '$command' @_\n";
    }
    return $output unless $exitcode;

    print STDERR join( ' ',
        @_, "died with exit status $exitcode (" . ( $exitcode >> 8 ) . ")\n" );

    # First 8 bits of exitcode report startup failures
    # Next ones report actual return code
    if ( $exitcode == -1 ) {
        print STDERR "failed to execute: $!\n";
        POSIX::_exit($exitcode);
    }
    elsif ( $exitcode & 127 ) {
        printf STDERR "child died with signal %d, %s coredump\n",
          ( $exitcode & 127 ),
          ( $exitcode & 128 ) ? 'with' : 'without';
        POSIX::_exit($exitcode);
    }
    POSIX::_exit( $exitcode >> 8 );
    return $output;
}

# Check for existence of directory (if status is 256 or 512)
sub check_target_exists {
    my ( $kernel, $heap, $session, $task_id, $status ) =
      @_[ KERNEL, HEAP, SESSION, ARG0, ARG1 ];

    my $arg    = $heap->{arg}->{$task_id};
    my $header = $arg->{header};

    if ( ( $status == 256 || $status == 512 ) ) {
        my $targetdir = dirname( $arg->{target} );

       # check if directory was already created by other file copy error handler
        if ( exists $heap->{created_target_dir}->{$targetdir} ) {

            # assume the directory has been created and retry
            $kernel->yield( 'rfcp_retry_handler', $task_id, $status, 1, 1 );
        }
        else {
            $heap->{Self}
              ->Quiet("$header Checking if directory $targetdir exists\n");
            my $dir_status =
              T0::Castor::RfstatHelper::checkDirExists($targetdir);

            # The target doesn't exists. Create the directory
            if ( $dir_status == 1 ) {
                $heap->{Self}->Quiet("$header Creating directory $targetdir\n");
                my @args;
                if ( $targetdir =~ m/^\/castor/ ) {
                    @args = ( "nsmkdir", "-m", "775", "-p", $targetdir );
                }
                else {
                    @args = ( "rfmkdir", "-m", "775", "-p", $targetdir );
                }
                if ( system(@args) == 0 ) {
                    $heap->{created_target_dir}->{$targetdir} = 1;
                    $kernel->yield( 'rfcp_retry_handler', $task_id, $status, 1,
                        0 );
                }
                else {

                    # something went wrong in directory creation
                    # normal retry, hope works better next round
                    $heap->{Self}->Quiet(
                        "$header Could not create directory $targetdir\n");
                    $kernel->yield( 'rfcp_retry_handler', $task_id, $status, 0,
                        0 );
                }
            }
            elsif ( $dir_status == 2 ) {

                # The targetdir is not a dir. Stop the iteration
                $heap->{Self}->Quiet("$header $targetdir is not a directory\n");
            }

            # Target exists and it is a directory
            elsif ( $dir_status == 0 ) {
                $heap->{Self}->Quiet("$header Directory $targetdir exists\n");
                $kernel->yield( 'rfcp_retry_handler', $task_id, $status, 0, 1 );
            }
        }
    }

    # no problems with target, regular retry
    else {
        $kernel->yield( 'rfcp_retry_handler', $task_id, $status, 0, 1 );
    }
}

# Remove target file if it exists only if I didn't create the target
# directory in the previous step
sub rfcp_retry_handler {
    my ( $kernel, $heap, $session, $task_id, $status, $createdTargetDir,
        $deleteTargetFile )
      = @_[ KERNEL, HEAP, SESSION, ARG0 .. ARG3 ];

    my $arg    = $heap->{arg}->{$task_id};
    my $header = $arg->{header};

    if ( $arg->{inputhash}->{delete_bad_files} && $deleteTargetFile == 1 ) {
        $heap->{Self}->Quiet("$header Deleting file before retrying\n");

        if ( $arg->{target} =~ m/^\/castor/ ) {
            qx{stager_rm -M $arg->{target} 2>/dev/null};
            qx{nsrm -f $arg->{target} 2>/dev/null};
        }
        else {
            qx{rfrm $arg->{target} 2>/dev/null};
        }
    }

# After creating the dir we retry without waiting and without decreasing retries
    if ( $createdTargetDir == 1 ) {
        $heap->{Self}->Quiet( "$header Created dir: (was $task_id) " .
              keys( %{ $heap->{task} } ) . " => $arg->{id}?\n" );
        $kernel->yield( 'start_wheel' => $arg, $task_id );
    }
    elsif ( $arg->{retries} > 0 ) {    # Retrying
        my $backoff_time = 0;
        if ( exists $arg->{retry_backoff} ) {
            if ( !ref( $arg->{retry_backoff} ) ) {    # Constant
                $backoff_time = $arg->{retry_backoff};
            }
            elsif ( ref( $arg->{retry_backoff} ) eq 'ARRAY' ) {    # Fibonacci
                $backoff_time = shift @{ $arg->{retry_backoff} };
            }
            else {    # Bogus value, logging error
                $heap->{Self}->Quiet( "$header No idea what RetryBackoff is:"
                      . " $arg->{retry_backoff}, ignoring\n" );
            }
        }
        $heap->{Self}->Quiet(
            "$header Retry count at $arg->{retries}, retrying",
            (
                $backoff_time
                ? " in $backoff_time seconds"
                : ' immediately'
            ),
            "\n"
        );
        $arg->{retries}--;

        if ($backoff_time) {
            $kernel->delay_set(
                'start_wheel' => $backoff_time,
                $arg, $task_id
            );
        }
        else {
            $kernel->yield( 'start_wheel' => $arg, $task_id );
        }
    }
    else {    # No more retries
        $heap->{Self}
          ->Quiet("$header Retry count at $arg->{retries}, giving up!\n");
        $kernel->yield( 'cleanup_task' => $task_id, $status );
    }
}

# Do something with all POE events which are not caught
sub handle_default {
    my ( $kernel, $event, $args ) = @_[ KERNEL, ARG0, ARG1 ];
    Croak(  "WARNING: Session "
          . $_[SESSION]->ID
          . " caught unhandled event $event with (@$args)." );
}

1;
