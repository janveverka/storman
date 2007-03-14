use strict;
package T0::DAQOps;
use DBI;


our $verbose = 1;
our $debug   = 0;

sub parse_daq_output{
  my ($arr, $cmd) = @_;
  my ($h, $l, $i);
  my %h;
  
  for $l (split (/\n/,`$cmd`)){
    $debug && print "$_\n";

    $h = undef; 	

    for $_ (split(/\s+/, $l)) {
      m%(.*)=(.*)%;
      if ($1 && $2) {
        $h =  {n => $i} unless $h; 
        $h->{$1} = $2 
      };
      $debug && print "$h->{$1} = $2\n";
    };

    $h && ++$i;
    push @{$arr}, $h;
  };
  
  $verbose && print @$arr[0], "\n";
  map {print "$_ -> $$arr[0]->{$_}\n" } keys %{$$arr[0]};		
};

sub update_safety {
  my ($fname, $level) = @_;
  my $cmd = "perl /home/klute/tier0/perl/updateSafety.pl $fname $level 2>&1";
  my $res;
  
  $verbose && print "Executing $cmd\n";

  $res = `$cmd`;

  return ($res);
};


#sub simple_query {
#        my ($h, $arr) = shift;
#        my $tmphref;
#
#        # Connect to DB
#        my $dbi    = "DBI:Oracle:omds";
#        my $reader = "cms_sto_mgr";
#        my $dbh = DBI->connect($dbi,$reader,"qwerty");
#
#
#        #
#        # Prepare sql query
#        #
#
#        return 1 unless keys %$h;
#
#        # Getting "Where" block
#        my $where_block = " WHERE ";
#        map {$where_block .= "$_ LIKE '%$h->{$_}%'  AND "} keys %$h;
#        $where_block =~ s/ AND $//;
#
#        # Building query
#        #my $SQLQUERY = "SELECT  RUNNUMBER, LUMISECTION, INSTANCE, COUNT, TYPE, STREAM, STATUS, SAFETY, NEVENTS, FILESIZE, HOSTNAME, PATHNAME, FILENAME FROM CMS_STO_MGR_ADMIN.RUN_FILES $where_block"; #WHERE STATUS = '$STATUS'";
#
#        my $SQLQUERY = "SELECT *  FROM CMS_STO_MGR_ADMIN.RUN_FILES $where_block";
#
#        print "Query is $SQLQUERY\n";
#        my $sth = $dbh->prepare($SQLQUERY);
#
#        # Execute the SQL
#        $sth->execute() || die $dbh->errstr;
#
#        # Parse the result
#        my @row;
#        while ($tmphref = $sth->fetchrow_hashref) {
#                push @$arr, $tmphref;
#                $tmphref = undef;
#        };
#
#        $verbose && print "First row is:\n";
#        $verbose && map {print "$_ ->".($$arr[0]->{$_} || "NULL")."\n" } keys %{$$arr[0]};
#
#        # Disconnect from DB
#        $dbh->disconnect;
#
#};

1;
