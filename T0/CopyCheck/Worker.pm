use strict;
use warnings;
package T0::CopyCheck::Worker;
use POE;
use POE::Filter::Reference;
use POE::Component::Client::TCP;
use Sys::Hostname;
use File::Basename;
use XML::Twig;
use T0::Util;
use T0::Castor::RfstatHelper;

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
				 check_size => 'check_size',
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
  $heap->{TargetDir} = $self->{TargetDir};
  $heap->{Node} = $self->{Node};

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
    my $work = $hash_ref->{work};
    my $pfn = $work->{PFN};

    $heap->{HashRef} = $hash_ref;

    # SvcClass checked in Manager already
    $ENV{STAGE_SVCCLASS} = $work->{SvcClass};

    # mark start time
    #$heap->{WorkStarted} = time;

    # Call rfstat and check the file.
    # RfstatRetries is the number of retries in case of receiving error from rfstat command.
    # RfstatRetryBackoff is the delay between tries.
    my $size = T0::Castor::RfstatHelper::getFileSize( $hash_ref->{work}->{PFN}, $self->{RfstatRetries}, $self->{RfstatRetryBackoff} );

    #$kernel->yield('check_size', ($size));

    my $status = check_size($self,$work->{PFN},$work->{FILESIZE},$self->{RfstatRetries},$self->{RfstatRetryBackoff});

    $hash_ref->{status} = $status;

    if ( $status == 0 )
      {
	# check on index
	if ( defined $work->{INDEXSIZE} and defined $work->{INDEXPFN} )
	  {
	    if ( 1 == check_size($self,$work->{INDEXPFN},$work->{INDEXSIZE},$self->{RfstatRetries},$self->{RfstatRetryBackoff}) )
	      {
		$hash_ref->{status} = 1;
	      }
	    elsif ( defined $work->{INDEXPFNBACKUP} )
	      {
		if ( 1 == check_size($self,$work->{INDEXPFNBACKUP},$work->{INDEXSIZE},$self->{RfstatRetries},$self->{RfstatRetryBackoff}) )
		  {
		    $hash_ref->{status} = 1;
		  }
	      }
	  }

	# delete file if DeleteAfterCheck flag is set
	if (defined($work->{DeleteAfterCheck}) && $work->{DeleteAfterCheck} == 1)
	  {
	    delete_file($pfn);

	    if ( defined $work->{INDEXSIZE} and defined $work->{INDEXPFN} )
	      {
		delete_file($work->{INDEXPFN});

		if ( defined $work->{INDEXPFNBACKUP} )
		  {
		    delete_file($work->{INDEXPFNBACKUP});
		  }
	      }
	  }
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


# call rfstat, compare size
sub check_size
{
  my $self = shift;
  my $pfn = shift;
  my $expectedSize = shift;
  my $retries = shift;
  my $backoff = shift;

  my $status = 1;

  my $filesize = T0::Castor::RfstatHelper::getFileSize($pfn,$retries,$backoff);

  if ( $filesize == -1 )
    {
      $self->Quiet("rfstat failed for ", $pfn, ".\n");
    }
  elsif ( $filesize == $expectedSize )
    {
      $self->Quiet($pfn, " size matches.\n");
      $status = 0;
    }
  else
    {
      $self->Quiet($pfn, " size doesn't match\n");
    }

  return $status;
}

# delete file
sub delete_file
{
  my $pfn = shift;

  if ( $pfn =~ m/^\/castor/ )
    {
      qx {stager_rm -M $pfn};
      qx {nsrm $pfn};
    }
  else
    {
      qx {rfrm $pfn};
    }
}

1;
