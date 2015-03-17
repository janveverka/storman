########################################################################################################
#
# Tools to use Rfstat in an easier way
#
########################################################################################################


use strict;
use warnings;
package T0::Castor::RfstatHelper;
use T0::Util;
use T0::Castor::XrdStatLite;

our (@ISA, @EXPORT, @EXPORT_OK, %EXPORT_TAGS, $VERSION);

use Carp;
$VERSION = 1.00;
@ISA = qw/ Exporter /;

our $hdr = __PACKAGE__ . ':: ';
sub Croak   { croak $hdr,@_; }
sub Carp    { carp  $hdr,@_; }
sub Verbose { T0::Util::Verbose( (shift)->{Verbose}, ("RFSTATHELPER:\t", @_) ); }
sub Debug   { T0::Util::Debug(   (shift)->{Debug},   ("RFSTATHELPER:\t", @_) ); }
sub Quiet   { T0::Util::Quiet(   (shift)->{Quiet},   ("RFSTATHELPER:\t", @_) ); }


# Check if the file argument exists
# Return status:
# 0 -> Pfn exists.
# 1 -> Pfn doesn't exists.
sub checkFileExists {

  my $pfn = shift;
  my $retries = shift;
  my $retry_backoff = shift;

  my ( $status, $stats_number, $stats_fields, $stats_data ) = 
    T0::Castor::RfstatLite->new( $pfn, $retries, $retry_backoff );

  if( $status == 0 ) {
    return 0;
  }
  else {
    return 1;
  }

}


# Check if the dir argument exists and it is a dir.
# Return status:
# 0 -> Pfn exists (and it is a dir).
# 1 -> Pfn doesn't exists.
# 2 -> Pfn exists but it isn't a dir.
sub checkDirExists {

  my $pfn = shift;

  my ( $status, $stats_number, $stats_fields, $stats_data ) = 
    T0::Castor::RfstatLite->new( $pfn, 5, 2 );

  # Target does not exist
  if ( $status != 0 )
    {
      return 1;
    }
  else
    {
      # Target exists and it is a directory
      if ( $stats_data->{'Flags'} =~ /.*IsDir.*/ )
	{
	  return 0;
	}
      # Target exists and it is not a directory
      else
	{
	  return 2;
	}
    }
}


# Get the size of the given file.
# Return the size of the file
# or -1 if something went wrong.
sub getFileSize {

  my ( $pfn, $retries, $retries_backoff) = @_;

  my ( $status, $stats_number, $stats_fields, $stats_data );

  if ( !defined($retries)){
    ( $status, $stats_number, $stats_fields, $stats_data ) = 
      T0::Castor::XrdStatLite->new( $pfn, 5 );
  }
  elsif ( !defined($retries_backoff)){
    ( $status, $stats_number, $stats_fields, $stats_data ) = 
      T0::Castor::XrdStatLite->new( $pfn, $retries );
  }
  else {
    ( $status, $stats_number, $stats_fields, $stats_data ) = 
      T0::Castor::XrdStatLite->new( $pfn, $retries, $retries_backoff );
  }

  if ( $status != 0 ) {
    return -1;
  }
  else{
    return $stats_data->{'Size'};
  }
}

1;
