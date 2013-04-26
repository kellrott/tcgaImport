#!/bin/bash

. build.conf

mkdir -p $DATADIR

./tcgaImport.py -t $DATADIR/tcga_uuid_map

for a in `cat $CANCER_LIST`; do 
	for a in `./tcgaImport.py -c $a`; do 
		$SUBMIT_EXE ./tcgaImport.sh $a ;
	done;
done

for a in `cat tcga_cancer.list`; do 
	$SUBMIT_EXE ./tcgaImportClinical.sh $a ;
done

