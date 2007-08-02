########################################################################################################
#
# Tools to use Rfstat in an easier way
#
########################################################################################################


use strict;
use warnings;
package T0::Castor::RfstatHelper;
use T0::Util;
use T0::Castor::RfstatLite;

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


# Check if the file argument exists
# Return the status of the rfstatus command
sub checkFileExists {

  my $pfn = shift;

  my ( $status, $stats_number, $stats_fields, $stats_data ) = 
    T0::Castor::RfstatLite->new( $pfn, 5 );

  return $status;

}


# Check if the dir argument exists and it is a dir.
# Return status:
# 0 -> Pfn exists (and it is a dir).
# 1 -> Pfn doesn't exists.
# 2 -> Pfn exists but it isn't a dir.
sub checkDirExists {

  my $pfn = shift;

  my ( $status, $stats_number, $stats_fields, $stats_data ) = 
    T0::Castor::RfstatLite->new( $pfn, 5 );

  # The target doesn't exists. Create the directory
  if ( $status != 0 )
    {
      return 1;
    }
  else
    {
      # The targetdir is not a dir. Stop the iteration
      if($stats_data->{'Protection'} =~ /^[^d]/ )
	{
	  return 2;
	}
      # Target exists and it is a directory
      else
	{
	  return 0;
	}
    }
}

1;
