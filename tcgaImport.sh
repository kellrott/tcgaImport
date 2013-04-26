#!/bin/bash
#$ -S /bin/bash
#$ -cwd

. build.conf

#just echo the hostname for debugging
hostname

mkdir -p ./check
if [ ! -e ./check/$1 ]; then
	./tcgaImport.py -u $WORKDIR/tcga_uuid_map -w $TMPDIR -m $MIRROR --download --outdir ./out/ -b $1
	if (($? == "0")); then 
		touch ./check/$1; 
	fi
fi
