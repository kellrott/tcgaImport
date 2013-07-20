#!/usr/bin/env python

import argparse
import drmaa
import csv
import os

def sub_job(sess, jt, cmd):
    jt.remoteCommand = os.path.abspath(cmd[0])
    jt.workingDirectory = os.getcwd()
    jt.args = cmd[1:] 
    return sess.runJob(jt)

def main(args):
    
    handle = open(args.cmd_file)
    reader = csv.reader(handle, delimiter=" ")
    
    cmds = []
    for row in reader:
        cmds.append(row)

    s=drmaa.Session()
    s.initialize()
    jt = s.createJobTemplate()
        
    jids = []
    while len(cmds):
        while len(jids) < args.nslots:
            c = cmds.pop()
            j =  sub_job(s, jt, c)
            print "submitted %s as %s" % (c, j)
            jids.append( j )
        
        while len(jids) == args.nslots:
            rm_list = []
            for j in jids:
                try:
                    retval = s.wait(j, 3)
                    print 'Job: ' + str(retval.jobId) + ' finished with status ' + str(retval.hasExited)
                    rm_list.append(j)
                except drmaa.errors.ExitTimeoutException:
                    pass
            for j in rm_list:
                jids.remove(j)
                
            
        
    s.synchronize(jids, drmaa.Session.TIMEOUT_WAIT_FOREVER, False)
    for curjob in jids:
        print 'Collecting job ' + curjob
        retval = s.wait(curjob, drmaa.Session.TIMEOUT_WAIT_FOREVER)
        print 'Job: ' + str(retval.jobId) + ' finished with status ' + str(retval.hasExited)
    
    s.deleteJobTemplate(jt)
    s.exit()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('cmd_file',help="Command")
    parser.add_argument("-n", "--nslots", type=int, default=5)
    args = parser.parse_args()
    main(args)
