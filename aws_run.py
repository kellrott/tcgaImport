#!/usr/bin/env python

import os
import sys
import time
import boto
import boto.ec2
import boto.manage.cmdshell
import argparse


HOME_DIR = "/home/ubuntu"
REPO_HOME = "https://github.com/kellrott/tcgaImport.git"

def main_launch(args):
    it = 't1.micro'
    # it = 'm1.small'
    ami = "ami-137bcf7a" # ubuntu 12.04
    instance_name = "myInstance"
    security_group = "all_open"
    # elastic_ip = "1.2.3.4"
     
    ec2c = boto.ec2.connection.EC2Connection(os.environ['AWS_ACCESS_KEY_ID'],os.environ['AWS_SECRET_ACCESS_KEY'])
     
    user_data = """
    wont work on ubuntu
    """
     
    r = ec2c.run_instances(ami, instance_type=it, key_name=args.keypair, user_data=user_data, security_groups=[security_group])
     
    time.sleep(5)
    i = r.instances[-1]
     
    ec2c.create_tags([i.id], {"Name": instance_name})
     
    print "waiting for AMI to start ..."
    while not i.update() == 'running':
        print ".",
        time.sleep(2)
     
    print " ... success!"
    print i.ip_address
     
    # print "associated elastic IP?"
    # print ec2c.associate_address(i.id, elastic_ip)

def main_run(args):
    ec2c = boto.ec2.connection.EC2Connection(os.environ['AWS_ACCESS_KEY_ID'],os.environ['AWS_SECRET_ACCESS_KEY'])

    rid_list = ec2c.get_all_instances(filters={"tag:Name" : "myInstance"})
    if len(rid_list) == 0:
        return 1

    rid = rid_list[0]
    instance = rid.instances[0]

    ## info: http://boto.s3.amazonaws.com/ref/manage.html
    cmd = boto.manage.cmdshell.sshclient_from_instance(instance,
                                                   args.keyfile,
                                                   user_name=args.username)
    print cmd.run("df -h")

    if not cmd.exists(os.path.join(HOME_DIR, "tcgaImport")):
        print cmd.run("sudo apt-get install -y git")
        print cmd.run("git clone %s" % (REPO_HOME))

    if not cmd.exists(os.path.join(HOME_DIR, "tcgaImport", "data")):
        cmd.run("mkdir %s" % (os.path.join(HOME_DIR, "tcgaImport", "data")))

    if not cmd.exists(os.path.join(HOME_DIR, "tcgaImport", "data", "tcga_uuid_map")):
        cmd.run("cd %s && ./tcgaImport.py download uuid -o data/tcga_uuid_map" % (os.path.join(HOME_DIR, "tcgaImport")))

    print cmd.run("cd %s && ./tcgaImport.sh %s" % (os.path.join(HOME_DIR, "tcgaImport"), args.base))



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--keypair")
    parser.add_argument("--keyfile")
    parser.add_argument("--username", default="ubuntu")
    
    subparsers = parser.add_subparsers(title="subcommand")


    parser_launch = subparsers.add_parser('launch')
    parser_launch.set_defaults(func=main_launch)

    parser_run = subparsers.add_parser('run')
    parser_run.add_argument("base")
    parser_run.set_defaults(func=main_run)


    args = parser.parse_args()

    sys.exit(args.func(args))


