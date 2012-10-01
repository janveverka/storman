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
use POSIX qw( strftime );
use Carp;

our ( @ISA, @EXPORT, @EXPORT_OK, %EXPORT_TAGS, $VERSION );

my $maxworkers = 3;           # Number of parallel workers
my $alias      = 'Castor';    # Session name

$VERSION = 1.00;
@ISA     = qw/ Exporter /;

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
    my ( $major, $minor, $revision, $qualifier ) = split( /[.-]/, $version );
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

    my $kernel = $poe_kernel;    # Global from POE::Kernel
    if ( $poe_kernel->alias_get($alias) ) {    # Session already exists
        $kernel->post( $alias => 'add_task' => $hash_ref );
    }
    else {                                     # Create session
        my $self = bless( {}, $class );
        POE::Session->create(
            inline_states => {
                _start              => \&start,
                add_task            => \&add_task,
                start_wheel         => \&start_wheel,
                monitor_task        => \&monitor_task,
                exit_handler        => \&exit_handler,
                check_target_exists => \&check_target_exists,
                retry_handler       => \&retry_handler,
                got_task_stdout     => \&got_task_stdout,
                got_task_stderr     => \&got_task_stderr,
                got_sigchld         => \&got_sigchld,
            },
            args => [ $hash_ref, $self ],
        );
    }

    return;    # No need to return anything
}

sub start {
    my ( $kernel, $heap, $hash_ref, $self ) = @_[ KERNEL, HEAP, ARG0, ARG1 ];
    $kernel->alias_set($alias);

    # remember reference to myself
    $heap->{Self} = $self;

    $kernel->yield( 'add_task' => $hash_ref );
}

sub add_task {
    my ( $kernel, $heap, $hash_ref ) = @_[ KERNEL, HEAP, ARG0 ];

    my %arg = %$hash_ref;    # Create our own local heap
    $arg{inputhash} = $hash_ref;    # remember hash reference

    # before spawning wheels, register signal handler
    $kernel->sig( CHLD => "got_sigchld" );

    # XXX This is not Wheel-safe, but shouldn't matter
    if ( defined $arg{svcclass} ) {
        $ENV{STAGE_SVCCLASS} = $arg{svcclass};
    }
    else {
        $heap->{Self}->Quiet("SvcClass not set, use t0input!\n");
        $ENV{STAGE_SVCCLASS} = 't0input';
    }

    # spawn wheels
    foreach my $file ( @{ $hash_ref->{files} } ) {

        # hash to be passed to wheel
        my %filehash = (
            original       => $file,
            checked_source => 0,
            %$file,
        );

        # ensure default values for some parameters
        for my $param (qw( retries retry_backoff timeout )) {
            $filehash{$param} =
              exists $hash_ref->{$param}
              ? $hash_ref->{$param}
              : 0
              unless defined $filehash{$param};
        }

        $kernel->yield( 'start_wheel' => \%filehash );
    }
}

# Pushes what needs to be done on a queue
sub start_wheel {
    my ( $kernel, $heap, $arg ) = @_[ KERNEL, HEAP, ARG0 ];

    # see if with or without checksum check
    if (   $use_checksum
        && $arg->{checksum}
        && $arg->{target} =~ m/^\/castor/ )
    {
        $arg->{Program} = sub {
            my $exitcode = -1;
            my @args = ( "nstouch", $arg->{target} );
            $exitcode = system(@args);
            if ( $exitcode == 0 ) {
                @args = (
                    "nssetchecksum", "-n", "adler32", "-k", $arg->{checksum},
                    $arg->{target}
                );
                $exitcode = system(@args);
            }
            if ( $exitcode == 0 ) {
                @args = ( "rfcp", $arg->{source}, $arg->{target} );
                $exitcode = system(@args);
            }
            if ( $exitcode == 0 ) {    # Do some sanity checks
                my $check = qx{nsls -l --checksum $arg->{target}};
                if ( ( $exitcode = $? ) == 0 ) {
                    chomp $check;
                    my ( $cSize, $cChecksum ) = $check =~ /^
                          \S+\s+      # rights
                          \d+\s+      # refcount
                          \S+\s+      # owner
                          \S+\s+      # group
                          (\d+)\s+    # size ($1)
                          \S+\s+      # month
                          \d+\s+      # day of the month
                          \S+\s+      # time
                          AD\s+       # Adler32 checksum
                          (\w+)\s+    # checksum
                          \S+         # filename
                          $/x;
                    if ( defined $cSize && defined $cChecksum ) {
                        if ( $cSize != $arg->{size} ) {
                            print "size $cSize != $arg->{size}!\n";
                            $exitcode = 123 << 8;
                        }
                        elsif ( $cChecksum ne $arg->{checksum} ) {
                            print "checksum $cChecksum != $arg->{checksum}!\n";
                            $exitcode = 122 << 8;
                        }
                    }
                    else {
                        print "$check does not match pattern. Retrying!\n";
                        $exitcode = 121 << 8;
                    }
                }
            }    # sanity checks

            # First 8 bits of exitcode report startup failures
            # Next ones report actual return code
            if ( $exitcode == -1 ) {
                print "failed to execute: $!\n";
                POSIX::_exit($exitcode);
            }
            elsif ( $exitcode & 127 ) {
                printf "child died with signal %d, %s coredump\n",
                  ( $exitcode & 127 ),
                  ( $exitcode & 128 ) ? 'with' : 'without';
                POSIX::_exit($exitcode);
            }
            POSIX::_exit( $exitcode >> 8 );
        };
        $arg->{ProgramArgs} = [];
    }
    else {
        $arg->{Program} = "rfcp";
        $arg->{ProgramArgs} = [ $arg->{source}, $arg->{target} ];
    }

    push @{ $heap->{task_list} }, $arg;
    $kernel->yield('next_task');

}

# Creates a new wheel to start the copy
sub next_task {
    my ( $kernel, $heap ) = @_[ KERNEL, HEAP ];
    while ( keys( %{ $heap->{task} } ) < $maxworkers ) {
        my $arg = shift @{ $heap->{task_list} };
        last unless defined $arg;

        $heap->{Self}->Quiet("Start copy $arg->{id}\n");

        #$ENV{STAGER_TRACE} = 3;
        #$ENV{RFIO_TRACE} = 3;
        my $task = POE::Wheel::Run->new(
            Program      => $arg->{Program},
            ProgramArgs  => $arg->{ProgramArgs},
            StdoutFilter => POE::Filter::Line->new(),
            StderrFilter => POE::Filter::Line->new(),
            StdoutEvent  => 'got_task_stdout',
            StderrEvent  => 'got_task_stderr',
        );
        my $id = $task->ID;
        $arg->{taskNum}              = keys( %{ $heap->{task} } );
        $arg->{header}               = $id . '-' . $arg->{taskNum};
        $heap->{task}->{$id}         = $task;
        $heap->{arg}->{$id}          = $arg;
        $heap->{pid}->{ $task->PID } = $task;
        $kernel->sig_child( $task->PID, "got_sigchld" );

        $heap->{Self}->Quiet("[$arg->{header}] Source: $arg->{source}\n");
        $heap->{Self}->Quiet("[$arg->{header}] Target: $arg->{target}\n");
        $heap->{Self}->Quiet("[$arg->{header}] Size: $arg->{size}\n");
        $heap->{Self}
          ->Quiet("[$arg->{header}] Adler32 checksum: $arg->{checksum}\n")
          if $arg->{checksum};

        # spawn monitoring thread
        if ( $arg->{timeout} ) {
            $arg->{alarm_id} = $kernel->delay_set(
                'monitor_task' => $arg->{timeout},
                $task->ID, 0
            );
        }
    }
}

# Ensure that after the timeout expires, task is finished, otherwise kill it
sub monitor_task {
    my ( $kernel, $heap, $task_id, $force_kill ) =
      @_[ KERNEL, HEAP, ARG0, ARG1 ];

    if ( my $task = exists $heap->{task}->{$task_id} ) {
        my $arg = $heap->{arg}->{$task_id};

        delete $arg->{alarm_id};

        if ($force_kill) {
            $task->kill(9);

            # cleanup task if it doesn't exit after another 10 seconds
            $kernel->delay_set( 'rfcp_exit_handler' => 10, $task_id, -1 );
        }
        else {
            $task->kill();

            # 10 seconds should be enough for task to exit
            $arg->{alarm_id} =
              $kernel->delay_set( 'monitor_task' => 10, $task_id, 1 );

        }
    }
}

# Copy emitted a line on STDOUT
sub got_task_stdout {
    my ( $kernel, $heap, $stdout, $task_id ) = @_[ KERNEL, HEAP, ARG0, ARG1 ];
    $kernel->call( got_task_output => $stdout, $task_id, 'STDOUT' );
}

# Copy emitted a line on STDERR
sub got_task_stderr {
    my ( $kernel, $heap, $stderr, $task_id ) = @_[ KERNEL, HEAP, ARG0, ARG1 ];
    $kernel->call( got_task_output => $stderr, $task_id, 'ERROR' );
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
      . " [$arg->{header}] $kind: $line\n";
    close($logfile);
}

# Copy has finished, child exited
sub got_sigchld {
    my ( $kernel, $heap, $child_pid, $status ) = @_[ KERNEL, HEAP, ARG1, ARG2 ];

    if ( my $task_id = delete $heap->{pid}->{$child_pid} ) {
        if ( exists $heap->{task}->{$task_id} ) {
            $kernel->yield( 'rfcp_exit_handler', $task_id, $status );
        }
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

    my $task = delete $heap->{task}->{$task_id};
    return unless $task;    # Shouldn't happen

    my $arg = $heap->{arg}->{$task_id};

    if ( exists $arg->{alarm_id} ) {
        $kernel->alarm_remove( delete $arg->{alarm_id} );
    }

    # update status in caller hash
    $arg->{original}->{status} = $status;

    if ( $status != 0 ) {    # Something went wrong
        $heap->{Self}->Quiet(
            "Rfcp of " . $arg->{source} . " failed with status $status\n" );

        # Check if the source file exists just the first time.
        if ( !$arg->{checked_source} ) {
            $heap->{Self}
              ->Quiet( "Checking if file " . $arg->{source} . " exists\n" );
            my $file_status =
              T0::Castor::RfstatHelper::checkFileExists( $arg->{source} );

            $arg->{checked_source} = 1;

            # Rfstat failed. Source doesn't exist
            # If the source file doesn't exist we have nothing else to do.
            if ( $file_status != 0 ) {
                $heap->{Self}->Quiet(
                    "Source file " . $arg->{source} . " does not exist\n" );
            }

            # Source exists
            # If it exists continue with the cleanup.
            else {
                $heap->{Self}
                  ->Quiet( "Source file " . $arg->{source} . " exists\n" );
                $kernel->yield( 'check_target_exists', $task_id, $status );
            }
        }
        else {
            $kernel->yield( 'check_target_exists', $task_id, $status );
        }
    }
    else {    # rfcp succeeded, just do some cleanup
        $heap->{Self}->Quiet("$arg->{source} successfully copied\n");
        delete $heap->{arg}->{$task_id};
    }

    # See if there is something more to be processed
    $kernel->yield('next_task');
}

# Check for existence of directory (if status is 256 or 512)
sub check_target_exists {
    my ( $kernel, $heap, $session, $task_id, $status ) =
      @_[ KERNEL, HEAP, SESSION, ARG0, ARG1 ];

    my $arg = $heap->{arg}->{$task_id};

    if ( ( $status == 256 || $status == 512 ) ) {
        my $targetdir = dirname( $arg->{target} );

       # check if directory was already created by other file copy error handler
        if ( exists $heap->{created_target_dir}->{$targetdir} ) {

            # assume the directory has been created and retry
            $kernel->yield( 'rfcp_retry_handler', $task_id, $status, 1, 1 );
        }
        else {
            $heap->{Self}->Quiet("Checking if directory $targetdir exists\n");
            my $dir_status =
              T0::Castor::RfstatHelper::checkDirExists($targetdir);

            # The target doesn't exists. Create the directory
            if ( $dir_status == 1 ) {
                $heap->{Self}->Quiet("Creating directory $targetdir\n");
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
                    $heap->{Self}
                      ->Quiet("Could not create directory $targetdir\n");
                    $kernel->yield( 'rfcp_retry_handler', $task_id, $status, 0,
                        0 );
                }
            }
            elsif ( $dir_status == 2 ) {

                # The targetdir is not a dir. Stop the iteration
                $heap->{Self}->Quiet("$targetdir is not a directory\n");
            }

            # Target exists and it is a directory
            elsif ( $dir_status == 0 ) {
                $heap->{Self}->Quiet("Directory $targetdir exists\n");
                $kernel->yield( 'rfcp_retry_handler',
                    ( $task_id, $status, 0, 1 ) );
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
      = @_[ KERNEL, HEAP, SESSION, ARG0, ARG1, ARG2, ARG3 ];

    my $arg = $heap->{arg}->{$task_id};

    if ( $heap->{delete_bad_files} && $deleteTargetFile == 1 ) {
        $heap->{Self}->Quiet("Deleting file before retrying\n");

        if ( $arg->{target} =~ m/^\/castor/ ) {
            qx {stager_rm -M $arg->{target} 2> /dev/null};
            qx {nsrm -f $arg->{target} 2> /dev/null};
        }
        else {
            qx {rfrm $arg->{target} 2> /dev/null};
        }
    }

# After creating the dir we retry without waiting and without decreasing retries
    if ( $createdTargetDir == 1 ) {
        $kernel->yield( 'start_wheel', $arg );
    }
    elsif ( $arg->{retries} > 0 ) {    # Retrying
        $heap->{Self}
          ->Quiet( "Retry count at " . $arg->{retries} . " , retrying\n" );
        $arg->{retries}--;

        my $backoff_time = 0;
        if ( exists $arg->{retry_backoff} ) {
            if ( !ref( $arg->{retry_backoff} ) ) {    # Constant
                $backoff_time = $arg->{retry_backoff};
            }
            elsif ( ref( $arg->{retry_backoff} ) eq 'ARRAY' ) {    # Fibonacci
                $backoff_time = shift @{ $arg->{retry_backoff} };
            }
            else {    # Bogus value, logging error
                $heap->{Self}->Quiet( "No idea what RetryBackoff is:"
                      . " $arg->{retry_backoff}, ignoring\n" );
            }
        }
        $kernel->delay_set( 'start_wheel' => $backoff_time, $arg );
    }
    else {            # No more retries
        $heap->{Self}
          ->Quiet( "Retry count at " . $arg->{retries} . " , abandoning\n" );
    }
}

1;
