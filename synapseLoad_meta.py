#!/usr/bin/env python


import os
import sys
import json
import re
from glob import glob
import synapseclient
import hashlib
from argparse import ArgumentParser


def get_md5(path):
    md5 = hashlib.md5()
    with open(path,'rb') as f:
        for chunk in iter(lambda: f.read(8192), ''): 
                md5.update(chunk)
        md5str = md5.hexdigest()
    return md5str

def log(message):
    sys.stderr.write(message + "\n")

meta_fields = {
    "dataSubType" : ["dataSubType" , "@id" ],
    "freeze" : ["freeze"],
    "whitelist" : ["whitelist"],
    "acronym" : [ "diseaseAbbr" ],
    "fileType" : [ "@type" ],
    "dataCenter" : ["center"],
    "sampleSetType" : ["sampleSetType"],
    "lastUpdated" : ["version"],
    "source" : ["source"] 
}
    
if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("src", help="Scan directory", default=None)
    parser.add_argument("--user", help="UserName", default=None)
    parser.add_argument("--password", help="Password", default=None)
    parser.add_argument("--project", help="Project", default=None)
    parser.add_argument("--acronym", help="Limit to one Acronym", default=None)

    args = parser.parse_args()
    
    syn = synapseclient.Synapse()
    syn.login(args.user, args.password)
    
    study_ids = {}
    
    for a in glob(os.path.join( args.src, "*.json")):
        log( "Loading:" + a )
        handle = open(a)
        meta = json.loads(handle.read())
        handle.close()
        
        if args.acronym is None or args.acronym == meta['annotations']['acronym']:
            dpath = re.sub(r'.json$', '', a)            
            name = meta['name']                                        
            query = "select * from entity where benefactorId=='%s' and name=='%s'" % (args.project, name)
            res = syn.query(query)
            #print meta['@id'], res
            if res['totalNumberOfResults'] != 0:
                log( "Found " + res['results'][0]['entity.id'] )                    
                ent = syn.get(res['results'][0]['entity.id'], downloadFile=False)
                if 'description' in meta:
                    ent['description'] = meta['description']
                if 'platform' in meta:
                    ent['platform'] = meta['platform']
                ent['species'] = 'Homo sapiens'
                ent['disease'] = 'cancer'
                syn.store(ent, forceVersion=False)
                annot = syn.getAnnotations(ent)
                for field in meta['annotations']:
                    d = meta['annotations'][field]
                    if isinstance(d, list):
                        annot[field] = d
                    else:
                        annot[field] = [d]
                print annot
                syn.setAnnotations(ent, annot)
                #sys.exit(0)


