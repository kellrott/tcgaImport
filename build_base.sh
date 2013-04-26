#!/bin/bash

for a in `cat tcga_cancer.list`; do 
	for a in `./tcgaImport.py -c $a`; do 
		qsub ./tcgaImport.sh $a ;
	done;
done
