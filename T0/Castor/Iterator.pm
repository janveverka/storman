use strict;
package T0::Castor::Iterator;

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

sub _init
{
  my $self = shift;

  my %h = @_;
  map { $self->{$_} = $h{$_} } keys %h;
  open NSLS, "nsls -lR $self->{Directory} |" or
		die "nsls: $self->{Directory}: $!\n";

  return $self;
}

sub new
{
  my $proto  = shift;
  my $class  = ref($proto) || $proto;
  my $parent = ref($proto) && $proto;
  my $self = {  };
  bless($self, $class);
  $self->_init(@_);
}

sub Next
{
  my $self = shift;
  my ($file,$size);

  $_ = <NSLS> || return undef;

  if ( m%^[-mrwx]+\s+\S+\s+\S+\s+\S+\s+(\S+\s+)\S+\s+\S+\s+\S+\s+(\S+)$% )
  {
    $size = $1;
    $file = $self->{dir} . '/' . $2;
    return ($file,$size) if wantarray();
    return $file;
  }

  if ( m%^(/castor/cern.ch/.+):$% ) { $self->{dir} = $1; }
  return $self->Next;
}

1;
