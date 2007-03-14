use strict;
package QueryFile;
use DBI;

my $verbose = 0;

sub simple_query {
	my ($h, $arr) = @_;
  		
	# Connect to DB
	my $dbi    = "DBI:Oracle:omds";
	my $reader = "cms_sto_mgr";
	my $dbh = DBI->connect($dbi,$reader,"qwerty") or warn "Cannot connect to the DB: $DBI::errstr\n";

	
	#
	# Prepare sql query
	#
	
	return 1 unless keys %$h;	

	# Getting "Where" block
	my $where_block = " WHERE ";
	map {$where_block .= " $_ = '$h->{$_}' AND "} keys %$h;
	$where_block =~ s/ AND $//;
	
	# Building query
	#my $SQLQUERY = "SELECT  RUNNUMBER, LUMISECTION, INSTANCE, COUNT, TYPE, STREAM, STATUS, SAFETY, NEVENTS, FILESIZE, HOSTNAME, PATHNAME, FILENAME FROM CMS_STO_MGR_ADMIN.RUN_FILES $where_block"; #WHERE STATUS = '$STATUS'";

	my $SQLQUERY = "SELECT *  FROM CMS_STO_MGR_ADMIN.RUN_FILES $where_block";

	$verbose && print "Query is $SQLQUERY\n";
	my $sth = $dbh->prepare($SQLQUERY) or warn "Cannot execute statement\n $SQLQUERY : $DBI::errstr\n";
	
	# Execute the SQL
	$sth->execute() or warn "Cannot execute statement \n $SQLQUERY :$dbh->errstr";
	
	
	# Parse the result
	while (my $tmphref = $sth->fetchrow_hashref) {
		print "$tmphref\n";
		push @$arr, $tmphref;
	};

	$verbose && print "First row is:\n"; 
	$verbose && map {print "$_ ->".($$arr[0]->{$_} || "NULL")."\n" } keys %{$$arr[0]};
	
	# Disconnect from DB
	$dbh->disconnect;

	return $arr;

}

1;
