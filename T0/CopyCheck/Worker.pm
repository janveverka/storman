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
				 check_file => 'check_file',
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



########################################################################################
# Call rfstat and check the file.
# RfstatRetries is the number of retries in case of receiving error from rfstat command.
# RfstatBackoff is the delay between tries.
# If DeleteAfterCheck is set the files will be removed on success check.
########################################################################################
sub check_file {
  my ( $self, $kernel, $heap, $session ) = @_[ OBJECT, KERNEL, HEAP, SESSION ];

  my $hash_ref = $heap->{HashRef};
  my $work = $hash_ref->{work};
  my $pfn = $work->{PFN};

  # nothing went wrong yet
  $hash_ref->{status} = 0;


  # check if file exists
  my @stats = qx {rfstat $pfn};
  
  if ( $? != 0 )
  {
      $self->Quiet("Rfstat failed, output follows\n");
      foreach my $stat ( @stats )
      {
	  $self->Quiet("RFSTAT: " . $stat . "\n");
      }

      # Check if we have to retry the command after the fail
      if ( defined($self->{RfstatRetriesOnThisFile}) && $self->{RfstatRetriesOnThisFile}>0 )
      {
	  $self->Quiet("Retrying rfstat on $pfn...\n");
	  $self->{RfstatRetriesOnThisFile}--;

	  if ( defined($self->{RfstatRetryBackoff}) )
	  {
	      $kernel->delay_set('check_file', $self->{RfstatRetryBackoff});
	  }
	  else
	  {
	      $kernel->yield('check_file');
	  }	  
	  return;
      }
      # Without more retries status is set
      else
      {
	  $self->Quiet("No more retries rfstat on $pfn...\n");
	  $hash_ref->{status} = 1;	  
      }
  }
  else
  {
      foreach my $stat ( @stats )
      {
	  if ( $stat =~ /^Size/)
	  {
	      chomp($stat);
	      my ($dummy,$size) = split (" : ",$stat);
	      
	      #if ( $size == $work->{FILESIZE} )
              if ( $size >= $work->{FILESIZE} and $size > 0 )
	      {
                  $work->{FILESIZE} = $size;

		  $heap->{Self}->Quiet($pfn, " size matches.\n");
		  
		  # delete file if DeleteAfterCheck flag is set
		  if (defined($work->{DeleteAfterCheck}) && $work->{DeleteAfterCheck} == 1)
		  {
		      if ( $pfn =~ m/^\/castor/ )
		      {
			  qx {stager_rm -M $pfn};
			  qx {nsrm $pfn};
		      }
		      else
		      {
			  qx {rfrm $pfn};
		      }
		      
		      $heap->{Self}->Quiet($pfn, " deleted.\n");
		  }
	      }
	      else
	      {
		  $heap->{Self}->Quiet($pfn, " size doesn't match.\n");
		  $hash_ref->{status} = 1;
	      }
	  }
      }
  }
  
  $kernel->yield('job_done');
  
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
    $heap->{HashRef} = $hash_ref;

    # SvcClass checked in Manager already
    $ENV{STAGE_SVCCLASS} = $heap->{HashRef}->{work}->{SvcClass};

    # mark start time
    $heap->{WorkStarted} = time;

    if ( defined($self->{RfstatRetries}) )
    {
	$self->{RfstatRetriesOnThisFile} = $self->{RfstatRetries};
    }
    
    $kernel->yield('check_file');

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

1;
