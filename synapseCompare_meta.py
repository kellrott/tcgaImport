#!/usr/bin/env python

import synapseclient
import tcgaImport
from argparse import ArgumentParser
import json
import sys
import logging

logging.basicConfig(level=logging.DEBUG)


if __name__ == "__main__":

    parser = ArgumentParser()
    parser.add_argument("basename", nargs="?", default=None)
    parser.add_argument("-o", "--output", help="Output File", default=None)
    parser.add_argument("--user", help="UserName", default=None)
    parser.add_argument("--password", help="Password", default=None)
    parser.add_argument("--project", help="Project", default=None)

    args = parser.parse_args()


    if args.basename is not None:
        basename_list = [args.basename]
    else:
        basename_list = []
        for plat in tcgaImport.platform_list():
            logging.info("Queueing: %s" % (plat))
            basename_list += tcgaImport.archive_list(plat)

    if args.output is not None:
        handle = open(args.output, "w")
    else:
        handle = sys.stdout

    syn = synapseclient.Synapse()
    if args.user is not None and args.password is not None:
        syn.login(args.user, args.password)
    else:
        syn.login()


    for basename in basename_list:
        logging.info("Checking: %s" % (basename))
        basename_platform_alias = tcgaImport.get_basename_platform(basename)
        logging.info(basename_platform_alias)
        conf = tcgaImport.getBaseBuildConf(basename, basename_platform_alias, "./")
        request = conf.buildRequest()
        
        for subtypeName, subtypeData in tcgaImport.tcgaConfig[basename_platform_alias].dataSubTypes.items():
            filename = subtypeData['nameGen'](basename)
            entity_id = None
            for row in syn.query("select * from entity where benefactorId=='%s' and name=='%s'" % (args.project, filename))['results']:
                entity_id = row['entity.id']

            if entity_id is not None:
                ent = syn.getEntity(entity_id)
                prov = syn.getProvenance(ent)

                logging.info("Provenance: %s" % (prov))
                found_count = 0
                for req_elem in request['provenance']['used']:
                    found = False
                    for elem in prov['used']:
                        if req_elem['url'] == elem['url']:
                            found = True
                    if found:
                        found_count += 1
                    else:
                        logging.info("Not Found: %s" % (req_elem['url']))
                logging.info("Found %s of %s (%s)" % (found_count, len(prov['used']), len(request['provenance']['used'])) )
                if found_count == len(prov['used']) and found_count == len(request['provenance']['used']):
                    handle.write("READY: %s\n" % (basename)) 
                else:
                    handle.write("UPDATE: %s\n" % (basename)) 
            else:
                handle.write("MISSING: %s\n" % (basename)) 

    handle.close()
