use strict;
use warnings;
package T0::Copy::RfcpLite;
use POE qw( Wheel::Run Filter::Line );
use File::Basename;

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
					 start_rfcp => \&start_rfcp,
					 cleanup_task => \&cleanup_task,
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

  # put callback session and method on heap
  $heap->{session} = $hash_ref->{session};
  $heap->{callback} = $hash_ref->{callback};

  # keep count on outstanding rfcp process
  $heap->{rfcp_active} = 0;

  $ENV{STAGE_SVCCLASS} = $heap->{svcclass} if ( defined $heap->{svcclass} );

  # execute rfcp processes (in sequence)
  foreach my $file ( @{ $hash_ref->{files} } )
    {
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

      $heap->{rfcp_active}++;
      $kernel->yield('start_rfcp',(\%filehash));
    }
}

sub start_rfcp {
  my ( $kernel, $heap, $file ) = @_[ KERNEL, HEAP, ARG0 ];

  qx( rfcp $file->{source} $file->{target} );
#  qx( echo $file->{source} $file->{target} );

  my $status = $?;

  $kernel->yield('cleanup_task',($file,$status));
}

sub cleanup_task {
  my ( $kernel, $heap, $file, $status ) = @_[ KERNEL, HEAP, ARG0, ARG1 ];

  $heap->{rfcp_active}--;

  # update status in caller hash
  $file->{original}->{status} = $status;

  if ( $status != 0 )
    {
      $heap->{Self}->Debug("Rfcp of $file failed with status $status\n");

      if ( $file->{retries} > 0 )
	{
	  $heap->{Self}->Debug("Retry count at " . $file->{retries} . " , retrying\n");

	  $file->{retries}--;

	  # check for existence of directory (if status is 256)
	  #
	  # FIXME: parse rfstat output to check if it's really a directory
	  #
	  if ( $status == 256 )
	    {
	      my $targetdir = dirname( $file->{target} );

	      my @temp = qx { rfstat $targetdir 2> /dev/null };

	      if ( scalar @temp == 0 )
		{
		  qx { rfmkdir -p $targetdir };
		}
	    }

	  if ( exists $file->{retry_backoff} )
	    {
	      $heap->{rfcp_active}++;
	      $kernel->delay_set('start_rfcp',$file->{retry_backoff},($file));
	    }
	  else
	    {
	      $heap->{rfcp_active}++;
	      $kernel->yield('start_rfcp',($file));
	    }
	}
      else
	{
	  $heap->{Self}->Debug("Retry count at " . $file->{retries} . " , abandoning\n");
	}
    }

  if ( $heap->{rfcp_active} == 0 )
    {
      $kernel->post( $heap->{session}, $heap->{callback}, $heap->{inputhash} );

      delete $heap->{inputhash};
      delete $heap->{Self};
      delete $heap->{svcclass};
      delete $heap->{session};
      delete $heap->{callback};
      delete $heap->{rfcp_active};
    }
}

1;
