# SM Repos

This list is extracted from /nfshome0/gdarlea/CMSSYSADMIN/storman/trunk.  This
area is used to build the RPMs of the Transfer System.  These RPMs are:

    cms_sm_copymanager
    cms_sm_copyworker
    cms_sm_injectworker
    cms_sm_multipath_conf
    cms_sm_nagios_plugins
    cms_sm_net_conf
    cms_sm_operations
    cms_sm_storagemanager

Historically, the content of the RPMs used to come from various locations. Below
is the list of these locations, seperately for each RPM.  It was extracted
from the corresponding mkrpm.sh files.

I exclude the following RPMs, which seem to be all more admin-related:

    cms_sm_multipath_conf
    cms_sm_nagios_plugins
    cms_sm_net_conf
    cms_sm_storagemanager

## cms_sm_copymanager
Goes to /opt/copymanger, COMP/T0 gets stripped.

    cvs.web.cern.ch/cvs/cgi-bin/viewcvs.cgi/
      COMP/T0/perl_lib/T0/Copy/Manager.pm
      COMP/T0/perl_lib/T0/Logger/
      COMP/T0/perl_lib/T0/Util.pm
      COMP/T0/perl_lib/T0/TransferStatus
      COMP/T0/perl_lib/T0/FileWatcher.pm
      COMP/T0/src/Logger
      COMP/T0/src/CopyManager/CopyManager.pl
      COMP/T0/src/TransferStatusManager
    /nfshome0/cmsprod/TransferTest/
      ApMon_perl-2.2.6/ApMon/ApMon.pm
      ApMon_perl-2.2.6/ApMon/ApMon/
      perl/HiRes.pm
      perl/POE.pm
      perl/POE/
      perl/XML/
    https://git.cern.ch/web/CMS-SMTransfer.git/ (?)
      storman/cms_sm_copymanager/Curses.pm

## cms_sm_copyworker
Goes to /opt/copyworker, COMP/T0 gets stripped.

    cvs.web.cern.ch/cvs/cgi-bin/viewcvs.cgi/
      COMP/T0/perl_lib/T0/Copy/Worker.pm
      COMP/T0/perl_lib/T0/Castor/
      COMP/T0/perl_lib/T0/Castor.pm
      COMP/T0/perl_lib/T0/Logger/
      COMP/T0/perl_lib/T0/Util.pm
      COMP/T0/perl_lib/T0/FileWatcher.pm
      COMP/T0/src/CopyManager/CopyWorker.pl
      COMP/T0/operations/sendNotification.pl
    /nfshome0/cmsprod/TransferTest/
      ApMon_perl-2.2.6/ApMon/ApMon.pm
      ApMon_perl-2.2.6/ApMon/ApMon/
      perl/HiRes.pm
      perl/POE.pm
      perl/POE/
      perl/XML/
    https://git.cern.ch/web/CMS-SMTransfer.git/ (?)
      storman/cms_sm_copymanager/Curses.pm

## cms_sm_injectworker
Goes to /opt/injectworker/inject.

    cvs.web.cern.ch/cvs/cgi-bin/viewcvs.cgi/
      EventFilter/StorageManager/scripts/inject
        (excluding EventFilter/StorageManager/scripts/inject/compat/)
      COMP/T0/perl_lib/T0/Logger/Sender.pm
      COMP/T0/perl_lib/T0/Util.pm

## cms_sm_operations
Goes to /opt/smops.

    cvs.web.cern.ch/cvs/cgi-bin/viewcvs.cgi/
      EventFilter/StorageManager/scripts/operations


