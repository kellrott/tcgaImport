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


def get_md5(path):
    md5 = hashlib.md5()
    with open(path,'rb') as f:
        for chunk in iter(lambda: f.read(8192), ''): 
                md5.update(chunk)
        md5str = md5.hexdigest()
    return md5str

def log(message):
    sys.stdout.write(message + "\n")

def find_child(syn, project, name):
    query = "select * from entity where parentId=='%s' and name=='%s'" % (project, name)
    res = syn.query(query)
    for i in res['results']:
        return i['entity.id']

class IDMapping:
    def __init__(self, syn, project):
        self.syn = syn
        self.project = project
    
    def getParent(self, meta):
        fid = find_child(self.syn, self.project, meta['annotations']['acronym'])
        if fid is None:
            return None
        pid = find_child(self.syn, fid, meta['platform'])
        return pid

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("src", help="Scan directory", default=None)
    parser.add_argument("--user", help="UserName", default=None)
    parser.add_argument("--password", help="Password", default=None)
    parser.add_argument("--project", help="Project", default=None)
    parser.add_argument("--skip-md5", help="Skip MD5", action="store_true", default=False)
    parser.add_argument("--push", help="Push", action="store_true", default=False)
    parser.add_argument("--new-only", help="Push only new files", action="store_true", default=False)
    parser.add_argument("--acronym", help="Limit to one Acronym", default=None)

    args = parser.parse_args()
    
    syn = synapseclient.Synapse()
    if args.user is not None and args.password is not None:
        syn.login(args.user, args.password)
    else:
        syn.login()
        
    study_ids = IDMapping(syn, args.project)
    
    for a in glob(os.path.join( args.src, "*.json")):
        log( "Loading:" + a )
        handle = open(a)
        meta = json.loads(handle.read())
        handle.close()
        
        if args.acronym is None or args.acronym == meta['annotations']['acronym']:
            
            dpath = re.sub(r'.json$', '', a)
            if os.stat(dpath).st_size > 0:                               
                query = "select * from entity where benefactorId=='%s' and name=='%s'" % (args.project, meta['name'])
                res = syn.query(query)
                #print meta['@id'], res
                if res['totalNumberOfResults'] == 0:
                    log( "not found:" + meta['name'] )
                    if args.push:
                        parentId= study_ids.getParent(meta)
                        if parentId is not None:
                            entity = synapseclient.File(dpath, name=meta['name'], parentId=parentId)
                            #entity['contentType']='text/csv'
                            entity = syn.store(entity)
                            print "Created ", entity['id']
                else:
                    ent_id = res['results'][0]['entity.id']
                    log( "Found: " + ent_id )
                    upload = False
                    
                    """
                    if 'entity.md5' not in res['results'][0] or res['results'][0]['entity.md5'][0] != meta['md5']:
                        log("MD5 Miss: " + ent_id)
                        upload = True
                    else:
                        log("MD5 Match: " + ent_id)
                    """
                    
                    """
                    if not args.skip_md5:
                        upload = False
                        ent = syn.getEntity( ent_id )
                        if 'md5' not in ent:
                            upload = True
                        else:
                            if os.path.basename(ent['files'][0]) != os.path.basename(dpath):
                                log("File Name Mismatch: %s -> %s" % (ent['files'][0], dpath))
                                upload = True
                            elif os.path.basename(ent['cacheDir']) == "archive.zip_unpacked":
                                log("Replacing old-style 'archive.zip'")
                                upload = True                            
                            else:
                                #syn_md5 = get_md5(os.path.join(ent['cacheDir'], ent['files'][0]))
                                syn_md5 = ent['md5']
                                if meta['md5'] != syn_md5:
                                    upload = True
                                else:
                                    log("MD5 Match: " + ent_id)
                    """
                        
                    if upload:
                        if args.push and not args.new_only: 
                            log("Uploading: " + ent_id)
                            #archive_path = "archive.zip"
                            #z = zipfile.ZipFile(archive_path, "w")
                            #z.write(dpath, os.path.basename(dpath))
                            #z.close()
                            entity=syn.downloadEntity(ent_id)
                            #entity['contentType']='text/csv'
                            entity = syn.store(entity)
                            #syn.uploadFile(entity, archive_path)
                            syn.uploadFile(entity, dpath)
                        else:
                            log("To Be Uploaded: " + ent_id + " : " + meta['name'])
                            
