#!/usr/bin/env python


import os
import sys
import json
import re
from glob import glob
import synapseclient
import hashlib
from argparse import ArgumentParser


def log(message):
    sys.stdout.write(message + "\n")
    
if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("src", help="Scan directory", default=None)
    parser.add_argument("--user", help="UserName", default=None)
    parser.add_argument("--password", help="Password", default=None)
    parser.add_argument("--project", help="Project", default=None)
    
    args = parser.parse_args()
    
    syn = synapseclient.Synapse()
    if args.user is not None and args.password is not None:
        syn.login(args.user, args.password)
    else:
        syn.login()
    
    study_ids = {}
    
    for a in glob(os.path.join( args.src, "*.json")):
        log( "Loading:" + a )
        handle = open(a)
        meta = json.loads(handle.read())
        handle.close()
        
        dpath = re.sub(r'.json$', '', a)        
                                    
        query = "select * from entity where benefactorId=='%s' and name=='%s'" % (args.project, meta["name"])
        res = syn.query(query)
        #print meta['@id'], res
        if res['totalNumberOfResults'] != 0:
            log( "Found " + res['results'][0]['entity.id'] )                    
            ent = syn.getEntity( res['results'][0]['entity.id'] )

            if 'provenance' in meta:
                used_refs = meta['provenance']['used']
                for u in used_refs:
                    if 'name' not in u and 'url' in u:
                        u['name'] = u['url']
                    
                if len(used_refs):
                    activity = synapseclient.Activity(
                        meta['provenance']['name'])
                    if 'description' in meta['provenance']:
                        activity['description'] = meta['provenance']['description']
                    activity['used'] = used_refs
                    #print json.dumps(activity, indent=4)
                    #print ent
                    #prov = syn.getProvenance(ent)
                    #print json.dumps(prov, indent=4)
                    syn.setProvenance(ent, activity)
                    #sys.exit(0)
