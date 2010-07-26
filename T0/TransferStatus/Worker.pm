use strict;
use warnings;
package T0::TransferStatus::Worker;
use POE;
use POE::Filter::Reference;
use POE::Component::Client::TCP;
use Sys::Hostname;
use T0::Util;
use DBI;
use Data::Dumper;

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
  my $class = shift;
  my $self = {};
  bless($self, $class);

  my %h = @_;
  map { $self->{$_} = $h{$_} } keys %h;

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
    Started        => \&start_task,
    Args => [ $self ],
    ObjectStates   => [
		       $self =>	[
				 server_input => 'server_input',
				 connected => 'connected',
				 connection_error_handler => 'connection_error_handler',
				 job_done => 'job_done',
				 get_work => 'get_work',
				]
	],
  );

  return $self;
}

sub start_task {
  my ( $heap, $self ) = @_[ HEAP, ARG0 ];

  # keep reference to myself
  $heap->{Self} = $self;

  # put some parameters on heap
  $heap->{DatabaseHandleLifetime} = $self->{DatabaseHandleLifetime};
  $heap->{DatabaseInstance} = $self->{DatabaseInstance};
  $heap->{DatabaseName} = $self->{DatabaseName};
  $heap->{DatabaseUser} = $self->{DatabaseUser};
  $heap->{DatabasePassword} = $self->{DatabasePassword};

  #initialize some parameters
  $heap->{State} = 'Created';
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

  map { $self->{Channels}->{$_} = 1; } @{$T0::System{Channels}}
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

sub send
{
  my ( $self, $heap, $ref ) = @_;
  if ( !ref($ref) ) { $ref = \$ref; }
  if ( $heap->{connected} && $heap->{server} )
  {
    $heap->{server}->put( $ref );
  }
}

sub Log
{
  my $self = shift;
  my $logger = $self->{Logger};
  $logger->Send(@_) if defined $logger;
}


sub _server_input { reroute_event( (caller(0))[3], @_ ); }
sub server_input {
  my ( $self, $kernel, $heap, $session, $hash_ref ) = @_[ OBJECT, KERNEL, HEAP, SESSION, ARG0 ];

  my $command  = $hash_ref->{command};

  $heap->{Self}->Verbose("from server: $command\n");
  if ( $command =~ m%Sleep% )
  {
    # ask again in 10 seconds
    $kernel->delay_set( 'get_work', 10 );
    return;
  }

  if ( $command =~ m%DoThis% )
  {
    # save hash reference on heap, needed in job_done
    $heap->{HashRef} = $hash_ref;

    # convinience variables
    my $fileStatus = $hash_ref->{fileStatus};
    my $fileList = $hash_ref->{fileList};

    # mark start time
    $heap->{WorkStarted} = time;

    # set to 1 if we have to commit anything
    #
    # we rollback otherwise, even though there won't
    # be anything to rollback (better safe than sorry)
    $hash_ref->{commit} = 0;

    # keep overall status
    $hash_ref->{status} = 0;

    #
    # Check database handle, connect if needed
    # Prepare all the selects and inserts
    #
    if ( ! $heap->{DatabaseHandle}
         || time() - $heap->{DatabaseHandleCreation} > $self->{DatabaseHandleLifetime} ) {

      if ( $heap->{DatabaseHandle} ) {
	$heap->{Self}->Quiet("Database handle timed out, disconnecting\n");
	eval { $heap->{DatabaseHandle}->disconnect() };
      }
      undef $heap->{DatabaseHandle};

      $heap->{DatabaseHandleCreation} = time();

      eval {
	$heap->{DatabaseHandle} = DBI->connect($heap->{DatabaseInstance},
					       $heap->{DatabaseUser},
					       $heap->{DatabasePassword},
					       {
						RaiseError => 1,
						AutoCommit => 0
					       });
      };

      # Check if handle is usable
      if ( ! $heap->{DatabaseHandle}
	   || (! eval { $heap->{DatabaseHandle}->ping() } || $@)
	   || (! eval { $heap->{DatabaseHandle}->do("select sysdate from dual") } || $@) ) {
	$heap->{Self}->Quiet("Failed connection to database\n");
	undef $heap->{DatabaseHandle};
      }

      if ( $heap->{DatabaseHandle} ) {
	  my $sql = "merge into " . $heap->{DatabaseName} . ".files_trans_new ";
	  $sql .= "using dual on (" . $heap->{DatabaseName} . ".files_trans_new.FILENAME = ?) ";
	  $sql .= "when matched then update set ITIME = CURRENT_TIMESTAMP ";
	  $sql .= "when not matched then insert (FILENAME,ITIME) values (?,CURRENT_TIMESTAMP)";
	  if ( ! ( $heap->{StmtInsertFileNew} = $heap->{DatabaseHandle}->prepare($sql) ) ) {
	      $heap->{Self}->Quiet("failed prepare : $heap->{DatabaseHandle}->errstr\n");
	      undef $heap->{DatabaseHandle};
	  }
      }

      if ( $heap->{DatabaseHandle} ) {
	  my $sql = "merge into " . $heap->{DatabaseName} . ".files_trans_copied ";
	  $sql .= "using dual on (" . $heap->{DatabaseName} . ".files_trans_copied.FILENAME = ?) ";
	  $sql .= "when matched then update set ITIME = CURRENT_TIMESTAMP ";
	  $sql .= "when not matched then insert (FILENAME,ITIME) values (?,CURRENT_TIMESTAMP)";
	  if ( ! ( $heap->{StmtInsertFileCopied} = $heap->{DatabaseHandle}->prepare($sql) ) ) {
	      $heap->{Self}->Quiet("failed prepare : $heap->{DatabaseHandle}->errstr\n");
	      undef $heap->{DatabaseHandle};
	  }
      }

      if ( $heap->{DatabaseHandle} ) {
	  my $sql = "merge into " . $heap->{DatabaseName} . ".files_trans_checked ";
	  $sql .= "using dual on (" . $heap->{DatabaseName} . ".files_trans_checked.FILENAME = ?) ";
	  $sql .= "when matched then update set ITIME = CURRENT_TIMESTAMP ";
	  $sql .= "when not matched then insert (FILENAME,ITIME) values (?,CURRENT_TIMESTAMP)";
	  if ( ! ( $heap->{StmtInsertFileChecked} = $heap->{DatabaseHandle}->prepare($sql) ) ) {
	      $heap->{Self}->Quiet("failed prepare : $heap->{DatabaseHandle}->errstr\n");
	      undef $heap->{DatabaseHandle};
	  }
      }

      if ( $heap->{DatabaseHandle} ) {
	  my $sql = "merge into " . $heap->{DatabaseName} . ".files_trans_inserted ";
	  $sql .= "using dual on (" . $heap->{DatabaseName} . ".files_trans_inserted.FILENAME = ?) ";
	  $sql .= "when matched then update set ITIME = CURRENT_TIMESTAMP ";
	  $sql .= "when not matched then insert (FILENAME,ITIME) values (?,CURRENT_TIMESTAMP)";
	  if ( ! ( $heap->{StmtInsertFileInserted} = $heap->{DatabaseHandle}->prepare($sql) ) ) {
	      $heap->{Self}->Quiet("failed prepare : $heap->{DatabaseHandle}->errstr\n");
	      undef $heap->{DatabaseHandle};
	  }
      }

      if ( $heap->{DatabaseHandle} ) {
	  my $sql = "merge into files_trans_inserted ";
	  $sql .= "using dual on ( files_trans_inserted.FILENAME = ?) ";
	  $sql .= "when matched then update set ITIME = CURRENT_TIMESTAMP ";
	  $sql .= "when not matched then insert (FILENAME,ITIME) values (?,CURRENT_TIMESTAMP)";
	  if ( ! ( $heap->{StmtInsertFileInserted} = $heap->{DatabaseHandle}->prepare($sql) ) ) {
	      $heap->{Self}->Quiet("failed prepare : $heap->{DatabaseHandle}->errstr\n");
	      undef $heap->{DatabaseHandle};
	  }
      }

      if ( $heap->{DatabaseHandle} ) {
	  my $sql = "merge into " . $heap->{DatabaseName} . ".files_trans_repacked ";
	  $sql .= "using dual on (" . $heap->{DatabaseName} . ".files_trans_repacked.FILENAME = ?) ";
	  $sql .= "when matched then update set ITIME = CURRENT_TIMESTAMP ";
	  $sql .= "when not matched then insert (FILENAME,ITIME) values (?,CURRENT_TIMESTAMP)";
	  if ( ! ( $heap->{StmtInsertFileRepacked} = $heap->{DatabaseHandle}->prepare($sql) ) ) {
	      $heap->{Self}->Quiet("failed prepare : $heap->{DatabaseHandle}->errstr\n");
	      undef $heap->{DatabaseHandle};
	  }
      }

      if ( $heap->{DatabaseHandle} ) {
	$heap->{Self}->Quiet("Connected to database\n");
      }
    }

    if ( $heap->{DatabaseHandle} ) {

      #
      # insert or update the appropriate table based on new status
      #
      my $sth = undef;
      if ( $fileStatus eq 'new' ) { $sth = $heap->{StmtInsertFileNew}; }
      elsif ( $fileStatus eq 'copied' ) { $sth = $heap->{StmtInsertFileCopied}; }
      elsif ( $fileStatus eq 'checked' ) { $sth = $heap->{StmtInsertFileChecked}; }
      elsif ( $fileStatus eq 'inserted' ) { $sth = $heap->{StmtInsertFileInserted}; }
      elsif ( $fileStatus eq 'repacked' ) { $sth = $heap->{StmtInsertFileRepacked}; }

      if ( defined $sth )
	{
	    my $tuples = undef;
	    my @tuple_status = undef;
	    eval {
		$sth->bind_param_array(1,\@$fileList);
		$sth->bind_param_array(2,\@$fileList);
		$tuples = $sth->execute_array( { ArrayTupleStatus => \@tuple_status } );
	    };

	    if ( ! $@ and $tuples ) {
		foreach ( @$fileList ) {
		    $heap->{Self}->Quiet("Updated transfer status for $_ to $fileStatus\n");
		}
		$hash_ref->{commit} = 1;
	    } else {
		for my $tuple (0..@$fileList-1) {
		    my $status = $tuple_status[$tuple];
		    if ( $status and ref $status and $status->[0] ) {
			$heap->{Self}->Quiet("Could not update transfer status for " . $fileList->[$tuple] . " to $fileStatus\n");
			$heap->{Self}->Quiet("ERROR : " . $status->[1] . "\n");
		    } else {
			$heap->{Self}->Quiet("Updated transfer status for " . $fileList->[$tuple] ." to $fileStatus (unconfirmed)\n");
			$hash_ref->{commit} = 1;
		    }
		}
		$hash_ref->{status} = 1;
	    }
	}
      else
	{
	   foreach ( @$fileList ) {
		    $heap->{Self}->Quiet("Could not update $_ to unknown status $fileStatus\n");
		}
	   $hash_ref->{status} = 1;
	}

      eval {
	if ( $hash_ref->{commit} == 1 ) {
	  $heap->{DatabaseHandle}->commit();
	} else {
	  $heap->{DatabaseHandle}->rollback();
	}
      };

      if ( $@ ) {
	$heap->{Self}->Quiet("Database Error: Commit or rollback failed!\n");
	$heap->{Self}->Quiet("Invalidating database handle!\n");
	$hash_ref->{status} = 1;
	eval { $heap->{DatabaseHandle}->disconnect() } if ( $heap->{DatabaseHandle} );
	undef $heap->{DatabaseHandle};
      }
    } else {
      $hash_ref->{status} = 1;
    }

    if ( $hash_ref->{status} == 0 ) {
      $heap->{Self}->Quiet("Status update for ", $hash_ref->{id}, " succeeded\n");
    } else {
      $heap->{Self}->Quiet("Status update for ", $hash_ref->{id}, " failed\n");
    }

    $kernel->yield('job_done');

    return;
  }

  if ( $command =~ m%Setup% )
  {
    $heap->{Self}->Quiet("Got $command...\n");
    my $setup = $hash_ref->{setup};
    $heap->{Self}->{Debug} && dump_ref($setup);
    map { $self->{$_} = $setup->{$_} } keys %$setup;

    if ( $heap->{State} eq 'Running' )
      {
	$kernel->yield('get_work');
      }
    return;
  }

  if ( $command =~ m%Start% )
  {
    $heap->{Self}->Quiet("Got $command...\n");
    if ( $heap->{State} eq 'Created' )
      {
	$heap->{State} = 'Running';
	$kernel->yield('get_work');
      }
    return;
  }

  if ( $command =~ m%Stop% )
  {
    $heap->{Self}->Quiet("Got $command...\n");
    $heap->{State} = 'Stop';
    return;
  }

  if ( $command =~ m%Quit% )
  {
    $heap->{Self}->Quiet("Got $command...\n");
    $heap->{State} = 'Quit';
    $kernel->yield('shutdown');
    return;
  }

  Print "Error: unrecognised input from server! \"$command\"\n";
  $kernel->yield('shutdown');
}

sub _connected { reroute_event( (caller(0))[3], @_ ); }
sub connected
{
  my ( $self, $heap, $kernel, $hash_ref ) = @_[ OBJECT, HEAP, KERNEL, ARG0 ];
  $self->Debug("handle_connect: from server: $hash_ref\n");
  my %text = (  'command'       => 'HelloFrom',
                'client'        => $self->{Name},
             );
  $self->send( $heap, \%text );
}

sub get_work
{
  my ( $self, $heap ) = @_[ OBJECT, HEAP ];

  $heap->{WorkRequested} = time;

  my %text = (
	      'command'      => 'SendWork',
	      'client'       => $self->{Name},
	     );
  $self->send( $heap, \%text );
}


sub job_done
{
  my ( $self, $heap, $kernel ) = @_[ OBJECT, HEAP, KERNEL ];

  # send report back to manager
  $heap->{HashRef}->{command} = 'JobDone';
  $self->send( $heap, $heap->{HashRef} );

  #delete $heap->{HashRef};

  if ( ($heap->{State} eq 'Running') )
    {
      $kernel->yield('get_work');
    }
  else
    {
      Print "Shutting down...\n";
      $kernel->yield('shutdown');
      exit;
    }
}

1;
