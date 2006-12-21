use strict;
package T0::Logger::Dashboard;
use T0::Util;
use ApMon;
use Sys::Hostname;
use Cwd;

our (@ISA, @EXPORT, @EXPORT_OK, %EXPORT_TAGS, $VERSION);

use Carp;
$VERSION = 1.00;
@ISA = qw/ Exporter /;
$Dashboard::Name = 'Logger::Dashboard';

our (@queue,%q);
our $hdr = __PACKAGE__ . ':: ';
sub Croak   { croak $hdr,@_; }
sub Carp    { carp  $hdr,@_; }
sub Verbose { T0::Util::Verbose( (shift)->{Verbose}, @_ ); }
sub Debug   { T0::Util::Debug(   (shift)->{Debug},   @_ ); }
sub Quiet   { T0::Util::Quiet(   (shift)->{Quiet},   @_ ); }

sub _init
{
  my $self = shift;

  $self->{Name} = $Dashboard::Name;
  $self->{DashboardHost} = 'lxarda12.cern.ch:18884';
  $self->{apmon}->{sys_monitoring} = 0;
  $self->{apmon}->{general_info}   = 0;
  $self->{MaxMsgRate}  = 120;

  my %h = @_;
  map { $self->{$_} = $h{$_}; } keys %h;
  $self->ReadConfig();

  $self->{apm} = new ApMon( { $self->{DashboardHost} => $self->{apmon} } );
  $self->{apm}->setMaxMsgRate($self->{MaxMsgRate});

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

sub Options
{
  my $self = shift;
  my %h = @_;
  map { $self->{$_} = $h{$_}; } keys %h;
}

our @attrs = ( qw/ Host Name Exe / );
our %ok_field;
for my $attr ( @attrs ) { $ok_field{$attr}++; }

sub AUTOLOAD {
  my $self = shift;
  my $attr = our $AUTOLOAD;
  $attr =~ s/.*:://;
  return unless $attr =~ /[^A-Z]/;  # skip DESTROY and all-cap methods
  Croak "AUTOLOAD: Invalid attribute method: ->$attr()" unless $ok_field{$attr};
  $self->{$attr} = shift if @_;
  return $self->{$attr};
}

sub Cluster
{
  my $self = shift;
  my $cluster = shift;
  if ( defined($cluster) )
  {
    $self->{Cluster} = $T0::System{Name} . '_' . $cluster;
  }
  return $self->{Cluster} if defined($self->{Cluster});
  Croak "Cluster not defined for $self\n";
}
  
sub BaseNode
{
  my $self = shift;
  return $self->{basenode} if defined($self->{basenode});

  my $host = $self->Host || hostname();
  $host =~ s%\..*$%%;
# 1154383200 is Tue Aug  1 00:00:00 2006
  $self->{basenode} = join('-',($host,$$,time-1154383200)) . '_';
  return $self->{basenode};
}

sub Lsf
{
  my $self = shift;
  return $self->{lsf} if defined($self->{lsf});

  $self->{lsf} = $ENV{LSB_JOBID} || '';
  if ( $self->{lsf} ) { $self->{lsf} = '_' . $self->{lsf}; }
  return $self->{lsf};
}

sub Start
{
  my $self = shift;
  $self->Send(
		"StepStart",	$self->Exe,
 		@_,
	      );
  $self->Quiet('Dashboard: Cluster=',$self->Cluster,
			 ' Node=',$self->Node,
			 " StepStart\n");
}

sub Stop
{
  my $self = shift;
  my ($exitcode,$exitreason);
  $exitcode = shift or -999;
  $exitreason = shift or 'no reason given';
  $exitreason = 'successful completion' unless $exitcode;
  $self->Send(
		"StepStop",	$self->Exe,
		"ExitCode",	$exitcode,
		"ExitReason",	$exitreason,
 		@_,
	      );
  $self->Quiet('Dashboard: Cluster=',$self->Cluster,
			 ' Node=',$self->Node,
			 " StepStop\n");
}

sub Step
{
  my $self = shift;
  my $step = shift;
  if ( defined($step) )
  {
    $self->{Step} = $step;
    $self->Node($self->{Step});

    $self->{apm}->addJobToMonitor($$, cwd, $self->Cluster, $self->Node);
    $self->Quiet('Dashboard: Cluster=',$self->Cluster,
			   ' Node=',$self->Node,
			   " Step defined\n");
  }
  return $self->{Step} if defined($self->{Step});
  Croak "Step not defined for $self\n";
}

sub Node
{
  my $self = shift;
  my $step = shift;
  if ( defined($step) )
  {
    $self->{Node} = $self->BaseNode . $step . $self->Lsf;
  }

  return $self->{Node};
}

sub ReadConfig
{
  my $self = shift;
  my $file = $self->{Config};
  T0::Util::ReadConfig($self);
}

sub Send
{
  my $self = shift;
  my $h;
  
  if ( scalar(@_) == 1 ) { $h = shift; }
  else
  {
    my %h = @_;
    $h = \%h;
  }

  $self->{apm}->sendParameters( $self->Cluster, $self->Node, %$h );
}

1;
