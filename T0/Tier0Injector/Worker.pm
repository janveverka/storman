use strict;

use warnings;
package T0::Tier0Injector::Worker;
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
    # ask again in 30 seconds
    $kernel->delay_set( 'get_work', $hash_ref->{wait} );
    return;
  }

  if ( $command =~ m%DoThis% )
  {
    # go through workList and process content
    my @streamList = ();
    my @versionList = ();
    my $run_hash_ref = {};
    my $lfn_hash_ref = {};
    foreach my $work ( @{$hash_ref->{workList}} ) {

	my $run = int($work->{RUNNUMBER});
	my $lumi = int($work->{LUMISECTION});
	my $stream = $work->{STREAM};
	my $version = $work->{APP_VERSION};
	my $hltkey = $work->{HLTKEY};
	my $starttime = int($work->{START_TIME});
	my $filesize = int($work->{FILESIZE});
	my $nevents = int($work->{NEVENTS});
	my $lfn = $work->{LFN};

	# HLTKEY missing for old messages
	$hltkey = 'none' unless $hltkey;

	# only keep base release from online version
	my @tempList = split('_', $version);
	$version = join('_', @tempList[0..3]);

	push(@streamList, $stream);
	push(@versionList, $version);

	if ( not exists $run_hash_ref->{$run} ) {
	    $run_hash_ref->{$run} = {};
	    $run_hash_ref->{$run}->{lumis} = {};
	    $run_hash_ref->{$run}->{streams} = {};
	    $run_hash_ref->{$run}->{version} = $version;
	    $run_hash_ref->{$run}->{hltkey} = $hltkey;
	    $run_hash_ref->{$run}->{starttime} = $starttime;
	}
	$run_hash_ref->{$run}->{lumis}->{$lumi} = 1;
	$run_hash_ref->{$run}->{streams}->{$stream} = 1;

	if ( not exists $lfn_hash_ref->{lfn} ) {
	    $lfn_hash_ref->{$lfn} = {};
	    $lfn_hash_ref->{$lfn}->{run} = $run;
	    $lfn_hash_ref->{$lfn}->{lumi} = $lumi;
	    $lfn_hash_ref->{$lfn}->{stream} = $stream;
	    $lfn_hash_ref->{$lfn}->{filesize} = $filesize;
	    $lfn_hash_ref->{$lfn}->{nevents} = $nevents;

	    # assume it did not work until proven otherwise
	    $lfn_hash_ref->{$lfn}->{status} = 1;
	}
    }
    delete $hash_ref->{workList};

    # mark start time
    $heap->{WorkStarted} = time;

#    my $checksum = $work->{CHECKSUM};
#    my $type = $work->{TYPE};

    # nothing has gone wrong yet
    $hash_ref->{status} = 0;

    #
    # Check database handle, connect if needed
    # Prepare all the selects and inserts
    #
    if ( ! $heap->{DatabaseHandle}
         || time() - $heap->{DatabaseHandleCreation} > $self->{DatabaseHandleLifetime} ) {

      if ( $heap->{DatabaseHandle} ) {
	$heap->{Self}->Quiet("Database handle timed out, disconnecting\n");
	$heap->{StmtFindCMSSWVersion} = undef;
	$heap->{StmtInsertCMSSWVersion} = undef;
	$heap->{StmtFindRun} = undef;
	$heap->{StmtInsertRun} = undef;
	$heap->{StmtInsertLumi} = undef;
	$heap->{StmtFindStream} = undef;
	$heap->{StmtInsertStream} = undef;
	$heap->{StmtFindRunStreamVersionAssoc} = undef;
	$heap->{StmtInsertRunStreamVersionAssoc} = undef;
	$heap->{StmtFindStreamer} = undef;
	$heap->{StmtInsertStreamer} = undef;
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
						PrintError => 0,
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
	my $sql = "SELECT COUNT(*) FROM " . $heap->{DatabaseUser} . ".cmssw_version ";
	$sql .= " WHERE name = ?";
	if ( ! ( $heap->{StmtFindCMSSWVersion} = $heap->{DatabaseHandle}->prepare($sql) ) ) {
	  $heap->{Self}->Quiet("failed prepare : ", $DBI::errstr, "\n");
	  undef $heap->{DatabaseHandle};
	}
      }

      if ( $heap->{DatabaseHandle} ) {
	my $sql = "INSERT INTO " . $heap->{DatabaseUser} . ".cmssw_version ";
	$sql .= "(ID,NAME) ";
	$sql .= "SELECT cmssw_version_SEQ.nextval, ? FROM DUAL ";
	$sql .= "WHERE NOT EXISTS ";
	$sql .= "(SELECT * FROM " . $heap->{DatabaseUser} . ".cmssw_version WHERE name = ?)";
	if ( ! ( $heap->{StmtInsertCMSSWVersion} = $heap->{DatabaseHandle}->prepare($sql) ) ) {
	  $heap->{Self}->Quiet("failed prepare : ", $DBI::errstr, "\n");
	  undef $heap->{DatabaseHandle};
	}
      }

      if ( $heap->{DatabaseHandle} ) {
	my $sql = "SELECT COUNT(*) FROM " . $heap->{DatabaseUser} . ".run a ";
	$sql .= "WHERE a.run_id = ?";
	if ( ! ( $heap->{StmtFindRun} = $heap->{DatabaseHandle}->prepare($sql) ) ) {
	  $heap->{Self}->Quiet("failed prepare : ", $DBI::errstr, "\n");
	  undef $heap->{DatabaseHandle};
	}
      }

      if ( $heap->{DatabaseHandle} ) {
	my $sql = "insert into " . $heap->{DatabaseUser} . ".run ";
	$sql .= "(RUN_ID,HLTKEY,RUN_VERSION,REPACK_VERSION,EXPRESS_VERSION,START_TIME,END_TIME,RUN_STATUS) ";
	$sql .= "VALUES (?,?,";
        $sql .= "(SELECT ID FROM " . $heap->{DatabaseUser} . ".cmssw_version WHERE NAME = ?),";
        $sql .= "(SELECT ID FROM " . $heap->{DatabaseUser} . ".cmssw_version WHERE NAME = ?),";
        $sql .= "(SELECT ID FROM " . $heap->{DatabaseUser} . ".cmssw_version WHERE NAME = ?),";
	$sql .= "?,0,";
	$sql .= "(SELECT ID FROM " . $heap->{DatabaseUser} . ".run_status WHERE STATUS = 'Active'))";
	if ( ! ( $heap->{StmtInsertRun} = $heap->{DatabaseHandle}->prepare($sql) ) ) {
	  $heap->{Self}->Quiet("failed prepare : ", $DBI::errstr, "\n");
	  undef $heap->{DatabaseHandle};
	}
      }

      if ( $heap->{DatabaseHandle} ) {
	my $sql = "INSERT INTO " . $heap->{DatabaseUser} . ".lumi_section ";
	$sql .= "(lumi_id,run_id) ";
	$sql .= "SELECT ?,? FROM DUAL ";
	$sql .= "WHERE NOT EXISTS ";
	$sql .= "(SELECT * FROM " . $heap->{DatabaseUser} . ".lumi_section ";
	$sql .= "WHERE lumi_id = ? AND run_id = ?)";
	if ( ! ( $heap->{StmtInsertLumi} = $heap->{DatabaseHandle}->prepare($sql) ) ) {
	  $heap->{Self}->Quiet("failed prepare : ", $DBI::errstr, "\n");
	  undef $heap->{DatabaseHandle};
	}
      }

      if ( $heap->{DatabaseHandle} ) {
	my $sql = "SELECT COUNT(*) FROM " . $heap->{DatabaseUser} . ".stream ";
	$sql .= "where name = ?";
	if ( ! ( $heap->{StmtFindStream} = $heap->{DatabaseHandle}->prepare($sql) ) ) {
	  $heap->{Self}->Quiet("failed prepare : ", $DBI::errstr, "\n");
	  undef $heap->{DatabaseHandle};
	}
      }

      if ( $heap->{DatabaseHandle} ) {
	my $sql = "INSERT INTO " . $heap->{DatabaseUser} . ".stream ";
	$sql .= "(ID,NAME) ";
	$sql .= "VALUES (stream_SEQ.nextval,?)";
	if ( ! ( $heap->{StmtInsertStream} = $heap->{DatabaseHandle}->prepare($sql) ) ) {
	  $heap->{Self}->Quiet("failed prepare : ", $DBI::errstr, "\n");
	  undef $heap->{DatabaseHandle};
	}
      }

      if ( $heap->{DatabaseHandle} ) {
	my $sql = "SELECT COUNT(*) FROM " . $heap->{DatabaseUser} . ".run_stream_cmssw_assoc ";
	$sql .= "WHERE run_id = ? ";
	$sql .= "AND stream_id = ( SELECT id FROM stream WHERE name = ? )";
	if ( ! ( $heap->{StmtFindRunStreamVersionAssoc} = $heap->{DatabaseHandle}->prepare($sql) ) ) {
	  $heap->{Self}->Quiet("failed prepare : ", $DBI::errstr, "\n");
	  undef $heap->{DatabaseHandle};
	}
      }

      if ( $heap->{DatabaseHandle} ) {
	my $sql = "INSERT INTO " . $heap->{DatabaseUser} . ".run_stream_cmssw_assoc ";
	$sql .= "(RUN_ID,STREAM_ID,RUN_VERSION,OVERRIDE_VERSION) ";
	$sql .= "SELECT ?,(SELECT id FROM stream WHERE name = ?),id,id FROM cmssw_version ";
	$sql .= "WHERE name = ?";
	if ( ! ( $heap->{StmtInsertRunStreamVersionAssoc} = $heap->{DatabaseHandle}->prepare($sql) ) ) {
	  $heap->{Self}->Quiet("failed prepare : ", $DBI::errstr, "\n");
	  undef $heap->{DatabaseHandle};
	}
      }

      if ( $heap->{DatabaseHandle} ) {
	my $sql = "DECLARE ";
	$sql .= "next_id INT; ";
	$sql .= "BEGIN ";
	$sql .= "SELECT wmbs_file_details_SEQ.nextval INTO next_id FROM DUAL; ";
	$sql .= "INSERT INTO " . $heap->{DatabaseUser} . ".wmbs_file_details ";
	$sql .= "(ID,LFN,FILESIZE,EVENTS,MERGED) ";
	$sql .= "VALUES (next_id,?,?,?,'1'); ";
	$sql .= "INSERT INTO " . $heap->{DatabaseUser} . ".wmbs_file_runlumi_map ";
	$sql .= "(FILEID,RUN,LUMI) ";
	$sql .= "VALUES (next_id,?,?); ";
	$sql .= "INSERT INTO " . $heap->{DatabaseUser} . ".streamer ";
	$sql .= "(STREAMER_ID,RUN_ID,LUMI_ID,INSERT_TIME,STREAM_ID) ";
	$sql .= "VALUES (next_id,?,?,?,(SELECT id FROM stream WHERE name = ?)); ";
	$sql .= "END;";
	if ( ! ( $heap->{StmtInsertStreamer} = $heap->{DatabaseHandle}->prepare($sql) ) ) {
	  $heap->{Self}->Quiet("failed prepare : ", $DBI::errstr, "\n");
	  undef $heap->{DatabaseHandle};
	}
      }

      if ( $heap->{DatabaseHandle} ) {
	$heap->{Self}->Quiet("Connected to database\n");
      }
    }

    if ( $heap->{DatabaseHandle} )
      {
	my $sth = undef;
	my $count = 0;

	#
	# insert cmssw version into T0AST if needed
	#
	if ( $hash_ref->{status} ==  0 ) {

	    $sth = $heap->{StmtFindCMSSWVersion};

	    my %hash = map { $_, 1 } @versionList;
	    @versionList = keys %hash;
	    my @insertVersionList = ();

	    # check which already exist
	    foreach my $version ( @versionList ) {

		eval {
		    $sth->bind_param(1, $version);
		    $sth->execute();
		    $count = $sth->fetchrow_array();
		};

		if ( $@ ) {
		    $heap->{Self}->Quiet("Could not check for CMSSW version $version\n");
		    $heap->{Self}->Quiet($DBI::errstr, "\n");
		    $hash_ref->{status} = 1;
		    last;
		}

		if ( $count == 0 ) {
		    push(@insertVersionList, $version);
		}
	    }

	    # insert what is needed
	    if ( $hash_ref->{status} == 0 and scalar(@insertVersionList) > 0 ) {

		$sth = $heap->{StmtInsertCMSSWVersion};

		my $tuples = undef;
		my @tuple_status = undef;
		eval {
		    $sth->bind_param_array(1, \@insertVersionList);
		    $sth->bind_param_array(2, \@insertVersionList);
		    $tuples = $sth->execute_array({ ArrayTupleStatus => \@tuple_status });
		};

		if ( ! $@ and $tuples ) {
		    foreach ( @insertVersionList ) {
			$heap->{Self}->Quiet("Inserted CMSSW version $_\n");
		    }
		} else {
		    foreach ( @insertVersionList ) {
			$heap->{Self}->Quiet("Could not insert CMSSW version $_\n");
		    }
		    $heap->{Self}->Quiet($DBI::errstr, "\n");
		    $hash_ref->{status} = 1;
		}

		eval {
		    if ( $hash_ref->{status} == 0 ) {
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
	    }
	}

	#
	# insert run into T0AST if needed
	#
	if ( $hash_ref->{status} ==  0 ) {

	    $sth = $heap->{StmtFindRun};

	    my @insertRunList = ();

	    # check which already exist
	    foreach my $run ( keys %$run_hash_ref ) {

		eval {
		    $sth->bind_param(1, $run);
		    $sth->execute();
		    $count = $sth->fetchrow_array();
		};

		if ( $@ ) {
		    $heap->{Self}->Quiet("Could not check for run $run\n");
		    $heap->{Self}->Quiet($DBI::errstr, "\n");
		    $hash_ref->{status} = 1;
		    last;
		}

		if ( $count == 0 ) {
		    push(@insertRunList, $run);
		}
	    }

	    # insert what is needed
	    if ( $hash_ref->{status} ==  0 and scalar(@insertRunList) > 0 ) {

		$sth = $heap->{StmtInsertRun};

		my @bindRunList = ();
		my @bindHltkeyList = ();
		my @bindVersionList = ();
		my @bindStarttimeList = ();

		foreach my $run ( @insertRunList) {
		    push(@bindRunList, $run);
		    push(@bindHltkeyList, $run_hash_ref->{$run}->{hltkey});
		    push(@bindVersionList, $run_hash_ref->{$run}->{version});
		    push(@bindStarttimeList, $run_hash_ref->{$run}->{starttime});
		}

		my $tuples = undef;
		my @tuple_status = undef;
		eval {
		    $sth->bind_param_array(1, \@bindRunList);
		    $sth->bind_param_array(2, \@bindHltkeyList);
		    $sth->bind_param_array(3, \@bindVersionList);
		    $sth->bind_param_array(4, \@bindVersionList);
		    $sth->bind_param_array(5, \@bindVersionList);
		    $sth->bind_param_array(6, \@bindStarttimeList);
		    $tuples = $sth->execute_array({ ArrayTupleStatus => \@tuple_status });
		};

		if ( ! $@ and $tuples ) {
		    foreach ( @insertRunList ) {
			$heap->{Self}->Quiet("Inserted run $_\n");
		    }
		} else {
		    foreach ( @insertRunList ) {
			$heap->{Self}->Quiet("Could not insert run $_\n");
		    }
		    $heap->{Self}->Quiet($DBI::errstr, "\n");
		    $hash_ref->{status} = 1;
		}

		eval {
		    if ( $hash_ref->{status} == 0 ) {
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
	    }
	}

	#
	# insert lumi section into T0AST if needed
	#
	# insert straight away, most likely won't exist already
	#
	if ( $hash_ref->{status} ==  0 ) {

	    $sth = $heap->{StmtInsertLumi};

	    my @insertRunList = ();
	    my @insertLumiList = ();

	    foreach my $run ( keys %$run_hash_ref ) {
		foreach my $lumi ( keys %{$run_hash_ref->{$run}->{lumis}} ) {
		    push(@insertRunList, $run);
		    push(@insertLumiList, $lumi);
		}
	    }

	    my $tuples = undef;
	    my @tuple_status = undef;
	    eval {
		$sth->bind_param_array(1, \@insertLumiList);
		$sth->bind_param_array(2, \@insertRunList);
		$sth->bind_param_array(3, \@insertLumiList);
		$sth->bind_param_array(4, \@insertRunList);
		$tuples = $sth->execute_array({ ArrayTupleStatus => \@tuple_status });
	    };

	    if ( ! $@ and $tuples ) {
		foreach my $run ( keys %$run_hash_ref ) {
		    foreach my $lumi ( keys %{$run_hash_ref->{$run}->{lumis}} ) {
			$heap->{Self}->Quiet("Inserted lumi section $lumi for run $run\n");
		    }
		}
	    } else {
		foreach my $run ( keys %$run_hash_ref ) {
		    foreach my $lumi ( keys %{$run_hash_ref->{$run}->{lumis}} ) {
			$heap->{Self}->Quiet("Could not insert lumi section $lumi for run $run\n");
		    }
		}
		$heap->{Self}->Quiet($DBI::errstr, "\n");
		$hash_ref->{status} = 1;
	    }

	    eval {
		if ( $hash_ref->{status} == 0 ) {
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
	}

	#
	# insert stream into T0AST if needed
	#
	if ( $hash_ref->{status} ==  0 ) {

	    $sth = $heap->{StmtFindStream};

	    my %hash = map { $_, 1 } @streamList;
	    @streamList = keys %hash;
	    my @insertStreamList = ();

	    foreach my $stream ( @streamList ) {

		eval {
		    $sth->bind_param(1, $stream);
		    $sth->execute();
		    $count = $sth->fetchrow_array();
		};
	    
		if ( $@ ) {
		    $heap->{Self}->Quiet("Could not check for stream $stream\n");
		    $heap->{Self}->Quiet($DBI::errstr, "\n");
		    $hash_ref->{status} = 1;
		    last;
		}

		if ( $count == 0 ) {
		    push(@insertStreamList, $stream);
		}
	    }

	    if ( $hash_ref->{status} ==  0 and scalar(@insertStreamList) > 0 ) {

		$sth = $heap->{StmtInsertStream};

		my $tuples = undef;
		my @tuple_status = undef;
		eval {
		    $sth->bind_param_array(1, \@insertStreamList);
		    $tuples = $sth->execute_array({ ArrayTupleStatus => \@tuple_status });
		};

		if ( ! $@ and $tuples ) {
		    foreach ( @insertStreamList ) {
			$heap->{Self}->Quiet("Inserted stream $_\n");
		    }
		} else {

		    # don't accept failure as-is, check if streams are all there
		    $heap->{Self}->Quiet("Error in bulk stream insertion\n");
		    $heap->{Self}->Quiet($DBI::errstr, "\n");
		    $heap->{Self}->Quiet("Double checking for completion\n");

		    $sth = $heap->{StmtFindStream};

		    foreach my $stream ( @insertStreamList ) {

			eval {
			    $sth->bind_param(1, $stream);
			    $sth->execute();
			    $count = $sth->fetchrow_array();
			};
	    
			if ( $@ ) {
			    $heap->{Self}->Quiet("Could not check for stream $stream\n");
			    $heap->{Self}->Quiet($DBI::errstr, "\n");
			    $hash_ref->{status} = 1;
			    last;
			}

			if ( $count == 0 ) {
			    $heap->{Self}->Quiet("Could not insert stream $stream\n");
			    $hash_ref->{status} = 1;
			} else {
			    $heap->{Self}->Quiet("Inserted stream $stream\n");
			}
		    }
		}

		eval {
		    if ( $hash_ref->{status} == 0 ) {
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
	    }
	}

	#
	# insert version for run/stream into T0AST if needed
	#
	if ( $hash_ref->{status} ==  0 ) {

	    $sth = $heap->{StmtFindRunStreamVersionAssoc};

	    my @insertRunList = ();
	    my @insertStreamList = ();
	    my @insertVersionList = ();

	    foreach my $run ( keys %$run_hash_ref ) {
		foreach my $stream ( keys %{$run_hash_ref->{$run}->{streams}} ) {

		    eval {
			$sth->bind_param(1, $run);
			$sth->bind_param(2, $stream);
			$sth->execute();
			$count = $sth->fetchrow_array();
		    };
	    
		    if ( $@ ) {
			$heap->{Self}->Quiet("Could not check for run_stream_cmssw_assoc for run $run and stream $stream\n");
			$heap->{Self}->Quiet($DBI::errstr, "\n");
			$hash_ref->{status} = 1;
		    }

		    if ( $count == 0 ) {
			push(@insertRunList, $run);
			push(@insertStreamList, $stream);
			push(@insertVersionList, $run_hash_ref->{$run}->{version});
		    }
		}
	    }

	    if ( $hash_ref->{status} == 0 and scalar(@insertRunList) > 0 ) {

		$sth = $heap->{StmtInsertRunStreamVersionAssoc};

		my $tuples = undef;
		my @tuple_status = undef;
		eval {
		    $sth->bind_param_array(1, \@insertRunList);
		    $sth->bind_param_array(2, \@insertStreamList);
		    $sth->bind_param_array(3, \@insertVersionList);
		    $tuples = $sth->execute_array({ ArrayTupleStatus => \@tuple_status });
		};

		if ( ! $@ and $tuples ) {
		    foreach my $run ( @insertRunList) {
			foreach my $stream ( keys %{$run_hash_ref->{$run}->{streams}} ) {
			    $heap->{Self}->Quiet("Inserted run_stream_cmssw_assoc for run $run and stream $stream\n");
			}
		    }
		} else {

		    # don't accept failure as-is, check if streams are all there
		    $heap->{Self}->Quiet("Error in bulk run_stream_cmssw_assoc insertion\n");
		    $heap->{Self}->Quiet($DBI::errstr, "\n");
		    $heap->{Self}->Quiet("Double checking for completion\n");

		    $sth = $heap->{StmtFindRunStreamVersionAssoc};

		    foreach my $run ( keys %$run_hash_ref ) {
			foreach my $stream ( keys %{$run_hash_ref->{$run}->{streams}} ) {

			    eval {
				$sth->bind_param(1, $run);
				$sth->bind_param(2, $stream);
				$sth->execute();
				$count = $sth->fetchrow_array();
			    };

			    if ( $@ ) {
				$heap->{Self}->Quiet("Could not check for run_stream_cmssw_assoc for run $run and stream $stream\n");
				$heap->{Self}->Quiet($DBI::errstr, "\n");
				$hash_ref->{status} = 1;
				last;
			    }

			    if ( $count == 0 ) {
				$heap->{Self}->Quiet("Could not insert run_stream_cmssw_assoc for run $run and stream $stream\n");
				$hash_ref->{status} = 1;
			    } else {
				$heap->{Self}->Quiet("Inserted run_stream_cmssw_assoc for run $run and stream $stream\n");
			    }
			}
		    }
		}
	    
		eval {
		    if ( $hash_ref->{status} == 0 ) {
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
	    }
	}
	#
	# insert streamer into T0AST if needed
	#
	if ( $hash_ref->{status} ==  0 ) {

	    # start assuming failure here, which means rollback
	    # unless something happens that we have to commit
	    $hash_ref->{status} =  1; 

	    $sth = $heap->{StmtInsertStreamer};

	    my @insertLfnList = ();
	    my @insertRunList = ();
	    my @insertLumiList = ();
	    my @insertStreamList = ();
	    my @insertFilesizeList = ();
	    my @insertNeventsList = ();

	    foreach my $lfn ( keys %$lfn_hash_ref ) {
		push(@insertLfnList, $lfn);
		push(@insertRunList, $lfn_hash_ref->{$lfn}->{run});
		push(@insertLumiList, $lfn_hash_ref->{$lfn}->{lumi});
		push(@insertStreamList, $lfn_hash_ref->{$lfn}->{stream});
		push(@insertFilesizeList, $lfn_hash_ref->{$lfn}->{filesize});
		push(@insertNeventsList, $lfn_hash_ref->{$lfn}->{nevents});
	    }

	    my $tuples = undef;
	    my @tuple_status = undef;
	    eval {
		$sth->bind_param_array(1, \@insertLfnList);
		$sth->bind_param_array(2, \@insertFilesizeList);
		$sth->bind_param_array(3, \@insertNeventsList);
		$sth->bind_param_array(4, \@insertRunList);
		$sth->bind_param_array(5, \@insertLumiList);
		$sth->bind_param_array(6, \@insertRunList);
		$sth->bind_param_array(7, \@insertLumiList);
		$sth->bind_param_array(8, time());
		$sth->bind_param_array(9, \@insertStreamList);
		$tuples = $sth->execute_array({ ArrayTupleStatus => \@tuple_status });
	    };

	    if ( ! $@ and $tuples ) {
		foreach ( keys %$lfn_hash_ref ) {
		    $heap->{Self}->Quiet("Inserted streamer with LFN $_\n");
		    $lfn_hash_ref->{$_}->{status} = 0;
		    $hash_ref->{status} = 0;
		}
	    } else {

		# bulk insert failed
		#
		# Now it gets tricky. As we use a procedure to insert, there is
		# no useful status information what rows were or weren't inserted
		#
		# this is error recovery, so we brute force it
		# first rollback, then go through the LFN list one by one
		#
		$heap->{Self}->Quiet("Error in bulk streamer insertion\n");
		$heap->{Self}->Quiet($DBI::errstr, "\n");
		$heap->{Self}->Quiet("Rollback and insert individually\n");

		eval {
		    $heap->{DatabaseHandle}->rollback();
		};

		if ( $@ ) {
		    $heap->{Self}->Quiet("Could not rollback failed bulk streamer insertion\n");
		    $heap->{Self}->Quiet($DBI::errstr, "\n");
		    $hash_ref->{status} = 1;
		}

		foreach my $lfn ( keys %$lfn_hash_ref ) {
		
		    eval {
			$sth->bind_param(1, $lfn);
			$sth->bind_param(2, $lfn_hash_ref->{$lfn}->{filesize});
			$sth->bind_param(3, $lfn_hash_ref->{$lfn}->{nevents});
			$sth->bind_param(4, $lfn_hash_ref->{$lfn}->{run});
			$sth->bind_param(5, $lfn_hash_ref->{$lfn}->{lumi});
			$sth->bind_param(6, time());
			$sth->bind_param(7, $lfn_hash_ref->{$lfn}->{filesize});
			$sth->bind_param(8, $lfn_hash_ref->{$lfn}->{nevents});
			$sth->bind_param(9, $lfn);
			$sth->bind_param(10, $lfn_hash_ref->{$lfn}->{stream});
			$tuples = $sth->execute();
		    };

		    if ( $@ ) {
			$heap->{Self}->Quiet("Could not insert streamer with LFN $lfn\n");
			$heap->{Self}->Quiet($DBI::errstr, "\n");
		    } else {
			$heap->{Self}->Quiet("Inserted streamer with LFN $lfn\n");
			$lfn_hash_ref->{$lfn}->{status} = 0;
			$hash_ref->{status} = 0;
		    }
		}
	    }

	    eval {
		if ( $hash_ref->{status} == 0 ) {
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
	}

	# should not be needed, for safety
	eval {
	  if ( $hash_ref->{status} == 0 ) {
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

    foreach my $lfn ( keys %$lfn_hash_ref ) {
	my $status = $lfn_hash_ref->{$lfn}->{status};
	if ( $status == 0 ) {
	    $heap->{Self}->Quiet("Injection for ", $lfn, " succeeded\n");
	} else {
	    $heap->{Self}->Quiet("Injection for ", $lfn, " failed\n");
	}
    }
    
    # cleanup lfn hash_ref
    delete $lfn_hash_ref->{run};
    delete $lfn_hash_ref->{lumi};
    delete $lfn_hash_ref->{stream};
    delete $lfn_hash_ref->{filesize};
    delete $lfn_hash_ref->{nevents};

    # save some information needed in job_done and Manager
    $heap->{HashRef} = $hash_ref;
    $heap->{HashRef}->{lfns} = $lfn_hash_ref;

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

  # send report back to manager, cleanup before
  $heap->{HashRef}->{command} = 'JobDone';
  $self->send( $heap, $heap->{HashRef} );

  if ( ($heap->{State} eq 'Running') ) {
      $kernel->yield('get_work');
  } else {
      Print "Shutting down...\n";
      $kernel->yield('shutdown');
      exit;
  }
}

1;
