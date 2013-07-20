#!/bin/bash
#$ -S /bin/bash
#$ -cwd

. build.conf

#just echo the hostname for debugging
hostname

mkdir -p ./check
if [ ! -e ./check/$1 ]; then
	./tcgaImport.py build -u $DATADIR/tcga_uuid_map -w $WORKDIR -m $MIRROR --checksum-delete --download --outdir $OUTDIR $1
	if (($? == "0")); then 
		touch ./check/$1; 
	fi
fi
