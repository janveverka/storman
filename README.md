# storman
The CMS Storage Manager Transfer System.

## History
The Perl modules in perl_lib were imported from the retired CVS
repository [1] on 18 March 2014.  Originally, they used to live at [2]
and were archived at [3].

They were imported using cvs2git bundled with CMSSW_6_2_7_patch1 using these
commands:

    MY_GITHUB_USER=`git config --get user.github`
    MY_REMOTE=git@github.com:$MY_GITHUB_USER/storman.git
    MY_PACKAGE=COMP/T0/perl_lib

    cvs2git --blobfile=git-blob.dat --dumpfile=git-dump.dat \
            /afs/cern.ch/project/cvs/reps/CMSSW/$MY_PACKAGE \
            --symbol-transform="(.*)/:\1-" \
            --use-external-blob-generator \
            --fallback-encoding "UTF8" \
            --username $USER

    git init storman
    cd storman
    git remote add origin $MY_REMOTE
    cat ../git-blob.dat ../git-dump.dat | git fast-import

See also [4].  The import is tagged "cvs2git."

- [1] /afs/cern.ch/project/cvs/reps/CMSSW/COMP/T0/perl_lib
- [2] http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/COMP/T0/perl_lib
- [3] http://cvs.web.cern.ch/cvs/cgi-bin/viewcvs.cgi/COMP/T0/perl_lib
- [4] http://cms-sw.github.io/cmssw/usercode-faq.html#how_do_i_migrate_to_github

