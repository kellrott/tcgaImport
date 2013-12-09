#!/bin/bash
#$ -S /bin/bash
#$ -cwd

#MAF_DIR=tcga_pancancer12_freeze_v4.0_mutations

. build.conf

hostname
#export TMPDIR=/scratch/tmp

#for a in `./tcgaImport.py list clinical`; do
a=$1
if [ ! -e ./check/$a ]; then
	./tcgaImport.py build -u $DATADIR/tcga_uuid_map -w $WORKDIR -m $MIRROR --outdir $OUTDIR -d $a
	if (($? == "0")); then 
		touch ./check/bio.$1; 
	fi
fi
#done

#./tcgaIDDag.py $1 > iddag/$1.iddag

#./mergeClinical.py clin/*${1}_bio.sample > clin/${1}.sample
#./mergeClinical.py clin/*${1}_bio.patient > clin/${1}.patient
#./mergeClinical.py clin/*${1}_bio.portion > clin/${1}.portion
#./mergeClinical.py clin/*${1}_bio.analyte > clin/${1}.analyte
#./mergeClinical.py clin/*${1}_bio.aliquot > clin/${1}.aliquot
#if test -e $(find clin/ -name "*.drug" -print -quit); then 
#	./mergeClinical.py clin/*${1}_bio.drug | ./flattenMatrix.py  drug_name total_dose > clin/${1}.drug
#fi

#./saturateClinical.py clin/${1}.sample iddag/${1}.iddag > clin_sat/${1}.sample
#./saturateClinical.py clin/${1}.patient iddag/${1}.iddag > clin_sat/${1}.patient
#./saturateClinical.py clin/${1}.portion iddag/${1}.iddag > clin_sat/${1}.portion
#./saturateClinical.py clin/${1}.analyte iddag/${1}.iddag > clin_sat/${1}.analyte
#./saturateClinical.py clin/${1}.aliquot iddag/${1}.iddag > clin_sat/${1}.aliquot

#if test -e clin/${1}.drug; then 
#	./saturateClinical.py clin/${1}.drug iddag/${1}.iddag > clin_sat/${1}.drug
#fi


#./tcgaMaf2Clinical.py -g KRAS,ERBB2,BRAF,NRAS,IGFR,BRCA,ALK,EGFR `./cgMeta.py -d $MAF_DIR -f "doc['diseaseAbbr']=='$1'" -pd | tr "\n" " " `  >  clin/${1}.mutation


#./mergeClinical.py clin/${1}.patient clin/${1}.mutation clin/${1}.drug*  > out/tcga_${1}_clinical.patient
#./cgMeta.py clin/*${1}*.json -n clinicalMatrix tcga_${1}_clinical.patient -p \
#--init "u={}" --end "new['sourceUrl'] = u.keys()" -e "for x in doc['sourceUrl']: u[x] = 1" \
#-m --merge -o out/tcga_${1}_clinical.patient.json 


#./mergeClinical.py clin_sat/${1}.* > out/tcga_${1}_clinical.aliquot

#./cgMeta.py clin/*${1}*.json -n clinicalMatrix tcga_${1}_clinical.aliquot -p \
#--init "u={}" --end "new['sourceUrl'] = u.keys()" -e "for x in doc['sourceUrl']: u[x] = 1" \
#-m --merge -o out/tcga_${1}_clinical.aliquot.json 
