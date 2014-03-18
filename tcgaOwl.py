#!/usr/bin/env python

import sys
import re
from glob import glob
from argparse import ArgumentParser
import logging

from rdflib import Namespace, BNode, Graph, Literal, URIRef
from rdflib.namespace import RDF, OWL

logging.basicConfig(level=logging.INFO)

TCGA_NS = Namespace("http://purl.org/bmeg/tcga/")
TCGA_OWL = Namespace("http://purl.org/bmeg/tcga.owl#")
BMEG_NS = Namespace("http://purl.org/bmeg/owl#")

tcga_pred_exclude = [ 
	TCGA_OWL.analysis, TCGA_OWL.sample, TCGA_OWL.radiation, 
	TCGA_OWL.followup, TCGA_OWL.gel_image_file, TCGA_OWL.tissue_source_site 
]


def main(args):
	out = Graph()

	p_map = {}
	for f in args.files:
		logging.info("Parsing: %s" % f)
		g = Graph()
		g.parse(f, format="turtle")
		for s, p, o in g:
			if isinstance(o, URIRef): 
				if p != RDF.type:
					if p not in tcga_pred_exclude:
						if p not in p_map:
							p_map[p] = {}
						p_map[p][o] = True
	for p in p_map:
		#print p
		pred = str(p).replace(str(TCGA_OWL), "")
		a = re.sub(r'^(.)', lambda match:match.group(1).upper(), pred )
		b = re.sub(r'_(.)', lambda match:match.group(1).upper(), a)
		#print TCGA_OWL[b]
		for o in p_map[p]:
			#print "\t", o
			out.add( (o, RDF.type, TCGA_OWL[b]))
			out.add( (TCGA_OWL[b], OWL.oneOf, o))

	print out.serialize(format="turtle")


if __name__ == "__main__":

    parser = ArgumentParser()
    #Stack.addJobTreeOptions(parser) 

    
    parser.add_argument("files", nargs="+")

    args = parser.parse_args()
    sys.exit(main(args))
