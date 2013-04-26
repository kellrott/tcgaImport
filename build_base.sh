#!/bin/bash

. build.conf

mkdir -p work
./tcgaImport.py -t $WORKDIR/tcga_uuid_map

for a in `cat $CANCER_LIST`; do 
	for a in `./tcgaImport.py -c $a`; do 
		$SUBMIT_EXT ./tcgaImport.sh $a ;
	done;
done
