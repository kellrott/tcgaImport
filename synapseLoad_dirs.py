#!/usr/bin/env python


import os
import sys
import json
import re
from glob import glob
import synapseclient
import hashlib
import zipfile
from argparse import ArgumentParser



def log(message):
    sys.stdout.write(message + "\n")

def find_child(syn, project, name):
    query = "select * from entity where parentId=='%s' and name=='%s'" % (project, name)
    res = syn.query(query)
    for i in res['results']:
        return i['entity.id']


def main():
    parser = ArgumentParser()
    parser.add_argument("src", help="Scan directory", default=None)
    parser.add_argument("--user", help="UserName", default=None)
    parser.add_argument("--password", help="Password", default=None)
    parser.add_argument("--project", help="Project", default=None)
    parser.add_argument("--push", help="Push", action="store_true", default=False)
    
    args = parser.parse_args()
    
    syn = synapseclient.Synapse()
    if args.user is not None and args.password is not None:
        syn.login(args.user, args.password)
    else:
        syn.login()
        
    folder_ids = {}
    
    for a in glob(os.path.join( args.src, "*.json")):
        log( "Loading:" + a )
        handle = open(a)
        meta = json.loads(handle.read())
        handle.close()
        
        dpath = re.sub(r'.json$', '', a)
                                
        query = "select * from entity where benefactorId=='%s' and name=='%s'" % (args.project, meta['name'])
        res = syn.query(query)
        #print meta['@id'], res
        if res['totalNumberOfResults'] == 0:
            log( "not found:" + meta['name'] )
            if meta['annotations']['acronym'] not in folder_ids:
                fid = find_child(syn, args.project, meta['annotations']['acronym'])
                if fid is None:
                    print "Create", meta['annotations']['acronym']
                    if args.push:
                        entityInfo={'entityType' : 'org.sagebionetworks.repo.model.Folder', u'name': meta['annotations']['acronym'], u'parentId': args.project}
                        entity = syn.createEntity(entityInfo)
                        fid = entity['id']
                        folder_ids[meta['annotations']['acronym']] = fid
                else:
                    folder_ids[meta['annotations']['acronym']] = fid
        
            else:
                fid = folder_ids[meta['annotations']['acronym']]
                
            folder_name = meta['annotations']['acronym'] + "/" + meta['platform']
            if folder_name not in folder_ids:
                pid = find_child(syn, fid, meta['platform'])
                if pid is None:
                    print "Create", folder_name
                    if args.push:
                        entityInfo={'entityType' : 'org.sagebionetworks.repo.model.Folder', u'name': meta['platform'], u'parentId': fid}
                        entity = syn.createEntity(entityInfo)
                        pid = entity['id']
                        folder_ids[folder_name] = pid
                else:
                    folder_ids[folder_name] = pid
            print folder_name
            

if __name__ == "__main__":
    main()
        