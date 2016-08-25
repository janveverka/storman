#!/usr/bin/env perl
# $Id: smCleanupFiles.pl,v 1.14 2012/02/07 10:38:49 gbauer Exp $
################################################################
################### Managed by puppet!!!  ######################
################################################################


use strict;
use warnings;
use DBI;
use Getopt::Long;
use File::Basename;
use File::Find ();
use POSIX;


my ($help, $debug, $nothing, $force, $execute, $maxfiles, $fileagemin, $skipdelete, $dbagemax, $dbrepackagemax0, $dbrepackagemax, $dbtdelete);
my ($hostname, $filename, $dataset, $config);
my ($runnumber, $uptorun, $safety, $rmexitcode, $chmodexitcode );
my ($constraint_runnumber, $constraint_uptorun, $constraint_filename, $constraint_hostname, $constraint_dataset);

my ($reader, $phrase);
my ($dbi, $dbh);
my ($fileageSMI);
my %h_notfiles;

my $month;


#-----------------------------------------------------------------
sub usage
{
  print "
  ##############################################################################

  Usage $0 [--help] [--debug] [--nothing] [--force] [--config=s] [--hostname=s]
           [--run=i] [--uptorun=i] [--filename=s] [--maxfiles=i]
           [--fileagemin=i] [--dbagemax=i] [--dbrepackagemax0=i] [--skipdelete] 
  Almost all the parameters were obvious at the time the developer wrote this,
  so they did not bother to actually write a help. Thank you so much! Guessing:

      help:            print this message and exit
      debug:           increase the verbosity of the log
      nothing:         do nothing?
      force:           do not interrupt execution even if you try to remove a
                          non-existing file
      config:          path to file with the DB credentials
      hostname:        delete files for other host than the one where the script is run
      run:             add a constraint to only query the DB for that particular runnumber
      uptorun:         add a constraint to only query the DB for runs smaller 
                          than that particular runnumber
      filename:        add a constraint to only query the DB for that file
      maxfiles:        max number of files out of DB query to process for DELETE
      fileagemin:      min age for a file to be deleted (min)
      dbagemax:        make DB query for files to delete out to dbagemax (days)
      dbrepackagemax0: max age for a file (after CHECK) before it gets deleted 
                          EVEN IF no REPACK! (days)
      skipdelete:      do not actually delete data files

  ##############################################################################
 \n";
  exit 0;
}

#-----------------------------------------------------------------
#subroutine for getting formatted time for SQL to_date method
sub gettimestamp($) {
    return strftime "%Y-%m-%d %H:%M:%S", localtime $_[0];
}

# Print out line preceded with current timestamp
sub printtime {
    my $date = strftime "%Y-%m-%d %H:%M:%S: ", localtime time;
    print "$date$_\n" for @_;
}

sub debug {
    if ($debug) { 
        my $time = strftime "%Y-%m-%d %H:%M:%S: ", localtime time;
        print "$time$_\n" for @_;
    }
}

#-----------------------------------------------------------------
sub deletefiles()
{

# Look for files in FILES_TRANS_CHECKED - implies closed and safety >= 100 in the old scheme.
# Alternate queries for different values of these? even needed?
# These files need to be in FILES_CREATED and FILES_INJECTED to
# check correct hostname and pathname. They must not be in FILES_DELETED.
#
# this query contains some optimizations from DB experts: do not muck with this lightly! [gb 02Jun2010]
    my $basesql = "select fi.PATHNAME, fc.FILENAME, fc.HOSTNAME, RUNNUMBER from CMS_STOMGR.FILES_CREATED fc " .
                      "inner join CMS_STOMGR.FILES_INJECTED fi " .
                              "on fc.FILENAME = fi.FILENAME    and  fi.ITIME > systimestamp - $dbagemax " .
                      "inner join CMS_STOMGR.FILES_TRANS_CHECKED ftc " .
                              "on fc.FILENAME = ftc.FILENAME   and ftc.ITIME > systimestamp - $dbagemax " .
                      "left outer join CMS_STOMGR.FILES_TRANS_REPACKED ftr   " . 
                              "on fc.FILENAME = ftr.FILENAME   and ftr.ITIME > systimestamp - $dbagemax " .
                      "left outer join CMS_STOMGR.FILES_DELETED fd " .
                              "on fc.FILENAME = fd.FILENAME " .
			      "where fc.CTIME > systimestamp - $dbagemax and  " .
                              "( ftr.FILENAME is not null or ftc.ITIME < systimestamp - $dbrepackagemax)" .
                              "and fd.FILENAME is null ";
 
# Sorting by time
    my $endsql = " order by fc.CTIME";
    
# Additional constraints
    $constraint_runnumber = '';
    $constraint_uptorun   = '';
    $constraint_filename  = '';
    $constraint_hostname  = '';
    $constraint_dataset   = '';
    
    if ($runnumber) { $constraint_runnumber = " and RUNNUMBER = $runnumber"; }
    if ($uptorun)   { $constraint_uptorun   = " and RUNNUMBER >= $uptorun";  }
    if ($filename)  { $constraint_filename  = " and CMS_STOMGR.FILES_TRANS_CHECKED.FILENAME = '$filename'";}
    if ($hostname)  { $constraint_hostname  = " and HOSTNAME = '$hostname'";}
    if ($dataset)   { $constraint_dataset   = " and SETUPLABEL = '$dataset'";}
    
# Compose DB query
    my $myquery = '';
    $myquery = "$basesql $constraint_runnumber $constraint_uptorun $constraint_filename $constraint_hostname $constraint_dataset $endsql";
    
    $debug && print "******BASE QUERY:\n   $myquery,\n";
    
        
    my $insertDel = $dbh->prepare("insert into CMS_STOMGR.FILES_DELETED (FILENAME,DTIME) VALUES (?,TO_DATE(?,'YYYY-MM-DD HH24:MI:SS'))");
    my $sth  = $dbh->prepare($myquery);
    
    
    
    debug "PreQuery...";
    
    $sth->execute() || die "Initial DB query failed: $dbh->errstr\n";
    
    debug "PostQuery...";

    $debug && print "******Runnumber:\n   $runnumber,\n";       

############## Parse and process the result
    my $nFiles   = 0;
    my $nRMFiles = 0;
    my $nRMind   = 0;
    
    
    
    my @row;
    
    $debug && print "MAXFILES: $maxfiles\n";


    my $statusQuery = '';
    my $statusResult;
    my $status = 1;

    while ( $nFiles<$maxfiles &&  (@row = $sth->fetchrow_array) ) {
         $statusQuery = "select STATUS from cms_stomgr.runs where runnumber = $row[3]";
         $statusResult  = $dbh->prepare($statusQuery);
         $statusResult->execute() || die "Initial DB query failed: $dbh->errstr\n";
         $status = $statusResult->fetchrow_array;


	$rmexitcode = 9999;    # Be over-cautious and set a non-zero default
	print "       --------------------------------------------------------------------\n";
	$nFiles++;
	
	# get .ind file name
	my $file =  "$row[0]/$row[1]";
  	print "          $file   \n";
	my $fileIND;
	if ( $file =~ /^(.*)\.(?:dat|root)$/ ) {
	    $fileIND = $1 . '.ind';
	}
       
	if ( -e $file ) {
	    my $FILEAGEMIN   =  (time - (stat(_))[9])/60;
	    print "file age: $FILEAGEMIN , remove if bigger than: $fileagemin\n";
           
	    if ($execute && $FILEAGEMIN > $fileagemin) {
		if ( unlink( $file ) == 1 ) {
		    # unlink should return 1: removed 1 file
		    $rmexitcode = 0;
                    if ( $status == 0 ) {
                        print "The run seems to be closed (status = 0), attempting to delete the jsn as well \n";
                        (my $file_jsn = $file) =~ s/.dat/.jsn/;
                        if ( unlink( $file_jsn ) == 1 ) {
                            print "jsn $file_jsn removed as well\n"
                        }
                        else {
                            print "jsn $file_jsn could not be removed\n"
                        }
                    }
		} else {
		    print "Removal of $file failed\n";
		    $rmexitcode = 9996;
		}
	    } elsif (!$execute && $FILEAGEMIN > $fileagemin) {
		#if we're not executing anything want to fake
		print "Pretending to remove $file\n";
                (my $file_jsn = $file) =~ s/.dat/.jsn/;
                print "Pretending to remove $file_jsn\n";
		$rmexitcode = 0;
	    } elsif ($FILEAGEMIN < $fileagemin) {
		print "File $file too young to die\n";
		$rmexitcode = 9995;
	    } else {
		print "This should never happen. File $file has issues!\n";
		$rmexitcode = 9994;
	    }
	} elsif ($force) {
	    print "File $file does not exist, but force=1, so continue\n";
	    $rmexitcode = 0;
	} elsif ( ! -d $row[0] ) {
	    print "Path $row[0] does not exist. Are the disks mounted?\n";
	    $rmexitcode = 9998;
	} else {
	    print "File $file does not exist\n";
	    $rmexitcode = 9997;
	}
       #$rmexitcode =0;
       # check file was really removed
	if ($rmexitcode != 0) {
	    print "Could not delete file: $file (rmexitcode=$rmexitcode)\n";
	} elsif ( ! -e $file ) {
	    $nRMFiles++;
	    
	    # insert file into deleted db
	    $insertDel->bind_param(1,$row[1]);
	    $insertDel->bind_param(2,gettimestamp(time));
	    $execute && ($insertDel->execute() || die "DB insert into deleted files failed: $insertDel->errstr\n");
	    my $delErr = $insertDel->errstr;
	    if(defined($delErr)) {
		print "Delete DB insert produced error: $delErr\n";
	    } else {
		print "File inserted into deleted DB, rm .ind\n";
		if ( $execute && -e $fileIND) {
		    my $rmIND = `rm -f $fileIND`;
		    if (! -e "$fileIND" ) {$nRMind++;}
		}
	    }
	} else {
	    print "Unlink returned success, but file $file is still there!\n";
	}
        $statusResult->finish();


}


# Only print summary if STDIN is a tty, so not in cron
if ($nFiles) {
    printtime "=================> DONE!:\n"
      . "BASE QUERY WAS:\n   $myquery,\n"
      . " $nFiles Files Processed\n"
      . " $nRMFiles Files rm-ed\n"
      . " $nRMind ind Files removed\n";
}
else {
    debug "=================> DONE!:\n"
      . " $nFiles Files Processed\n"
      . " $nRMFiles Files rm-ed\n"
      . " $nRMind ind Files removed\n";
}




#make sure handles are done
$sth->finish();


}

#-----------------------------------------------------------------
sub deleteCopyManager()
{
    my $dir="/store/copymanager/Logs";
    
    debug "search in $dir";
    if( -d $dir ){
	
	#my $string = `df $dir | grep dev`;
	my $string = `df $dir`;
	my ($favail) = ($string =~ /.*\ ([0-9]+)\%.*/);
	debug "----- Initial disk usage $favail\n";
	
	#delete older than 45 days:
	my $delete =`find /store/copymanager/Logs/*/ -mmin +64800  -type f   -exec sudo -u cmsprod  rm -f \'{}\' \\\; >& /dev/null`;
	
        #$string = `df $dir | grep dev`;
	$string = `df $dir`;
	($favail) = ($string =~ /.*\ ([0-9]+)\%.*/);
	debug "----- 45-day disk usage $favail\n";
	
	if( $favail > 50 ){
	    
	    #delete older than 32 days:
	    $delete = `find /store/copymanager/Logs/*/ -mmin +46080  -type f  -exec sudo -u cmsprod  rm -f \'{}\' \\\; >& /dev/null`;
	    
            #$string = `df $dir | grep dev`;
	    $string = `df $dir`;
	    ($favail) = ($string =~ /.*\ ([0-9]+)\%.*/);
	    print "-----   32-day disk usage $favail\n";
	    print "2: $delete\n" ;
	    
	    
	    
	    #brutal action: Manager files older than 20 days
	    if( $favail > 60 ){
		printtime "copymanager logs disk occupancy above 60: $delete";
		$delete = `find /store/copymanager/Logs/*/ -mmin +28800  -type f  -exec sudo -u cmsprod  rm -f \'{}\' \\\; >& /dev/null`;
		
		
		#brutal action: Manager files older than 15 days, and /tmp area older than 8 days
		if( $favail > 72 ){
		    
		    $delete = `find /store/copymanager/Logs/*/ -mmin +21600  -type f  -exec sudo -u cmsprod  rm -f \'{}\' \\\; >& /dev/null`;
		    printtime "copymanager logs disk occupancy above 72: $delete";
		    $delete = `sudo -u cmsprod find /tmp/* -mmin +11520  -type f  -exec sudo rm -f {} \; >& /dev/null`;
		    
		    
		    #brutal action: TransferStatusManager files are bigger and fatter, so tighterdelete: 7 days, and /tmp area older than 5 days
		    if( $favail > 83 ){
			
			$delete = `find /store/copymanager/Logs/TransferStatusManager/ -mmin +10080  -type f  -exec sudo -u cmsprod  rm -f \'{}\' \\\; >& /dev/null`;
                        printtime "copymanager logs disk occupancy above 83: $delete";
			$delete = `sudo -u cmsprod find /tmp/* -mmin +7200  -type f  -exec sudo rm -f {} \; >& /dev/null`;
			
			
			
			
			#emergency action: Manager files older than 2 days, and /tmp area older than 2 days
			if( $favail > 95 ){
			    
			    $delete = `find /store/copymanager/Logs/*/ -mmin +2880  -type f  -exec sudo -u cmsprod  rm -f \'{}\' \\\; >& /dev/null`;
			    printtime "copymanager logs disk occupancy above 95: $delete";
			    $delete = `find /tmp/* -mmin +2880  -type f  -exec sudo -u   rm -f {} \; >& /dev/null`;
			    
			}
			
		    }
		    
		    
		}
		
	    }
	    
	    
	}

        #$string = `df $dir | grep dev`;	
	$string = `df $dir`;
	($favail) = ($string =~ /.*\ ([0-9]+)\%.*/);
	debug "----- FINAL disk usage $favail\n";
    }
}


#-----------------------------------------------------------------
sub cleanInjectCopy()
{


  #dirs format: ["<dir path.", owner, disk-threshold, file-age-for delete]
   # [note: "/store/copyworker/workdir/" gets special dir-level treatment]
   # file-age in MONTHS!
    my @dirs = (["/store/injectworker/logs/", "smpro",             0.60,  2],
                ["/store/injectworker/logs/", "smpro",             0.81,  1],
#
                ["/store/copyworker/Logs/CopyManager/",  "cmsprod", 0.60, 2],
                ["/store/copyworker/Logs/CopyManager/",  "cmsprod", 0.81, 1],
#
                ["/store/copyworker/workdir/",  "cmsprod",          0.63, 2],
                ["/store/copyworker/workdir/",  "cmsprod",          0.85, 1]);
my $monthTOmins = 31*24*60; 

    my $ndir = @dirs;  #number of dir in above array:
 
#  sort dirs for deletions according to lowest disk threshold:
    my @odirs = sort {$a->[2] <=> $b->[2]} @dirs;

    
    
    for( my $i=0; $i<$ndir; $i++){
	
	if( -d $odirs[$i][0]){
	    
	    
	    debug "Cleaning:  $odirs[$i][0]";
	   
            #my $string = `df -h $odirs[$i][0] | grep dev`; 
	    my $string = `df -h $odirs[$i][0]`;
	    
	    #my ($favail) = ($string =~ /.*\ ([0-9]+)\%\ \//);
	    my ($favail) = ($string =~ /.*\ ([0-9]+)\%.*/);
	    $favail = 0.01*$favail;
	    debug "$i: ----- disk usage $favail vs current test: $odirs[$i][2]";
	    
	    if($favail < $odirs[$i][2] ) {last;}
	    
	    my $whom    = $odirs[$i][1];
	    my $fileage = int($monthTOmins*$odirs[$i][3]);
	    printtime "delete from dir $odirs[$i][0] at $odirs[$i][2] disk full and  $odirs[$i][3] months ($fileage min) \n";
	    
	    
	    my $dirappend = '*';
	    my $dirtype   = "f";
            my $depth     = "";
            my $rmop      = "-f";
	    if( $odirs[$i][0] =~ /.*copyworker\/workdir.*/ ){
		$dirappend = '0*';
		$dirtype   = "d";
                $depth     = ' -maxdepth 1 ';
                $rmop      = "-fr";
	    }
		
	    
	    my $string2= `ls -l $odirs[$i][0]$dirappend`;
	    print " $string2 <<<< \n";
	    
	    
	    
	    printtime "find $odirs[$i][0]$dirappend $depth   -mmin +$fileage  -type $dirtype \n";	    
	    my $delete =`find $odirs[$i][0]$dirappend $depth   -mmin +$fileage  -type $dirtype  `;	    
	    
	    printtime "\n $delete \n";
	    

#           imagine the full delete statement:
	    printtime "find $odirs[$i][0]$dirappend $depth  -mmin +$fileage  -type $dirtype  -exec sudo -u $whom   rm $rmop \'{}\' \\\;";

	    $delete = `find $odirs[$i][0]$dirappend $depth  -mmin +$fileage  -type $dirtype  -exec sudo -u $whom  rm $rmop \'{}\' \\\; `;

            printtime "\n $delete \n";

	    
	}
	
    }
    

	    debug 'Done' ;


    return;


}





#-----------------------------------------------------------------
sub maxdiskspace(){
    my $maxdisk=-1;

    #this was for the old SM, multiple sata* partitions were present, it was returning the biggest number. For the lustre implementation the "for" has 1 iteration
    for my $string (`df -h | grep "/store/lustre" `){
	my ( my $prcnt ) = ($string =~ /.+\ +.+\ +.+\ +.+\ +([0-9]+)\%.*/);
	if($maxdisk < $prcnt){ $maxdisk= $prcnt; }
    }
    
    return $maxdisk;
}
#-----------------------------------------------------------------

###################################################################
######################### MAIN ####################################

$help       = 0;
$debug      = 0;
$nothing    = 0;
$filename   = '';
$dataset    = '';
$uptorun    = 0;
$runnumber  = 0;
$safety     = 100;
$hostname   = '';
$execute    = 1;
$maxfiles   = 100000;     #     -- max number of files out of DB query to process for DELETE
$fileagemin     = 130;    #min  -- min age for a file to be deleted
$dbagemax       =  30;    #days -- make DB query for files to delete out to dbagemax 
$dbrepackagemax0=   7.01; #days -- max age for a file (after CHECK) before it gets deleted EVEN IF no REPACK!
$dbtdelete      =   6.0;  #hrs  -- cycle time to complete deletes over all nodes
$force      = 0;
$config     = "/opt/injectworker/.db.conf";
$fileageSMI  = 5;         #in days

$hostname   = `hostname -s`;
chomp($hostname);

GetOptions(
           "help"          =>\$help,
           "debug"         =>\$debug,
           "nothing"       =>\$nothing,
           "force"         =>\$force,
           "config=s"      =>\$config,
           "hostname=s"    =>\$hostname,
           "run=i"         =>\$runnumber,
	   "uptorun=s"	   =>\$uptorun,
	   "filename=s"	   =>\$filename,
	   "dataset=s"	   =>\$dataset,
           "maxfiles=i"    =>\$maxfiles,
           "fileagemin=i"  =>\$fileagemin,
           "dbagemax=i"    =>\$dbagemax,
           "dbrepackagemax0=i"    =>\$dbrepackagemax0,
           "skipdelete"    =>\$skipdelete
	  );

$help && usage;
if ($nothing) { $execute = 0; $debug = 1; }

my ($hour, $min);

$month = strftime "%b", localtime time;

#what's the current time at start of cycle; and *relative* to start of cycle:
$hour   = strftime "%H", localtime time;
$min    = strftime "%M", localtime time;

#check what is the occupancy percentage of lustre
my $maxdisk = maxdiskspace();

#set max age parameter for unrepacked files:
$dbrepackagemax = $dbrepackagemax0;
#OVERRIDE max age param for unrepacked files if disks getting too full
if   ( $maxdisk > 85 ){$dbrepackagemax =  0.04/24; $dbtdelete = 1.0; $fileagemin= 24*60*$dbrepackagemax ; } #
elsif( $maxdisk > 80 ){$dbrepackagemax =  0.15/24; $dbtdelete = 1.5; $fileagemin= 24*60*$dbrepackagemax ; } #
elsif( $maxdisk > 75 ){$dbrepackagemax =  0.25/24; $dbtdelete = 1.5; $fileagemin= 24*60*$dbrepackagemax ; } #
elsif( $maxdisk > 70 ){$dbrepackagemax =  1.0/24;  $dbtdelete = 1.5; $fileagemin= 24*60*$dbrepackagemax ; }
elsif( $maxdisk > 65 ){$dbrepackagemax =  2.0/24;  $dbtdelete = 3.0; }
elsif( $maxdisk > 60 ){$dbrepackagemax =  6.0/24;  $dbtdelete = 3.0; }
elsif( $maxdisk > 55 ){$dbrepackagemax = 12.0/24;  $dbtdelete = 3.0; }
elsif( $maxdisk > 50 ){$dbrepackagemax = 24.0/24;  $dbtdelete = 6.0; }
elsif( $maxdisk > 45 ){$dbrepackagemax = 48.0/24;  $dbtdelete = 6.0; }
elsif( $maxdisk > 40 ){$dbrepackagemax = 72.0/24;  $dbtdelete = 6.0; }
#else {  ;}

debug "*********************** $hostname ******  maxdisk: $maxdisk%  *********** ";
debug "dbrepackagemax = $dbrepackagemax,  dbtdelete = $dbtdelete, fileagemin = $fileagemin \n";
my ($rack, $node);
if       ( ( $rack, $node ) = ( $hostname =~ /mrg-c2f(12)-(\d+)-01/ ) ){ # main SM
    debug "I believe my hostname is $hostname; rack = $rack; node = $node. Proceeding with the cleanup!\n";
}
else {
    #unknown machine
    print "hostname not matching any pattern I'm aware of, exiting!";
    exit 0;
}

# Execute the following stuff ONLY IF there is a "sata" disk array
if( $maxdisk != -1 ) { 



$reader = "xxx";
$phrase = "xxx";
my ($hltdbi, $hltreader, $hltphrase);
if(-e $config) {
    eval `su smpro -c "cat $config"`;
} else {
    print "Error: Can not read config file $config, exiting!\n";
    usage();
}

$dbi    = "DBI:Oracle:cms_rcms";
$dbh    = DBI->connect($dbi,$reader,$phrase)
    or die "Can't make DB connection: $DBI::errstr\n";

#=======DELETE cycle for data files :

debug "..execute DELETES cycle on $hostname...";
if (!$skipdelete) { deletefiles(); }
debug "..DONE executing DELETES, maxdisk: $maxdisk...\n\n";

$dbh->disconnect;

}

debug "cleanup CopyManager";
deleteCopyManager(); 

debug ".. done with deleting copyManager logs....\n\n";


debug ".. move to remove inject/copy-worker logs....";
cleanInjectCopy();

debug ".....EXIT SCRIPT";
