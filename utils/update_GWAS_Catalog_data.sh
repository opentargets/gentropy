#!/usr/bin/env bash


# Function to get the most recent date:
get_most_recent(){
    cat $1 | perl -lane 'push @a, $_ if $_ =~ /^\d+$/; END {@a = sort { $a <=> $b} @a; print pop @a }'
}

# Function to return the path the to the most recent release:
get_release_url(){
    curl -s --list-only ${BASE_URL}/releases/ | get_most_recent | while read YEAR; do
        curl -s --list-only ${BASE_URL}/releases/${YEAR}/  | get_most_recent | while read MONTH; do
            DAY=$(curl -s --list-only ${BASE_URL}/releases/${YEAR}/${MONTH}/  | get_most_recent)
            echo $YEAR $MONTH $DAY
        done
    done
}

# Function to get the Ensembl and EFO version which used to ground GWAS data:
get_release_info(){
    curl -s https://www.ebi.ac.uk/gwas/api/search/stats | jq -r '"\(.ensemblbuild) \(.efoversion)"'
}

logging(){
    log_prompt="[$(date "+%Y.%m.%d %H:%M")]"
    echo "${log_prompt} $@"
}

# Resources:
export BASE_URL=ftp://ftp.ebi.ac.uk/pub/databases/gwas
export RELEASE_INFO_URL=https://www.ebi.ac.uk/gwas/api/search/stats
export GCP_TARGET=gs://genetics_etl_python_playground/input/v2d/

logging "Extracing data from: ${BASE_URL}"
logging "Release info fetched fom: ${RELEASE_INFO_URL}"
logging "Resulting files uploaded to: ${GCP_TARGET}"

# Capturing release date:
read YEAR MONTH DAY < <(get_release_url)
logging "Most recent GWAS Catalog release: ${YEAR}/${MONTH}/${DAY}"

# Capturing release metadata:
read ENSEMBL EFO < <(get_release_info)
logging "Genes were mapped to v${ENSEMBL} Ensembl release."
logging "Diseases were mapped to ${EFO} EFO release."

# Constructing FTP URL to access the most recent release:
RELEASE_URL=${BASE_URL}/releases/${YEAR}/${MONTH}/${DAY}
logging "Datafiles are fetching from ${RELEASE_URL}"

# Fetching files while assigning properly dated and annotated names:
wget -q ${RELEASE_URL}/gwas-catalog-associations_ontology-annotated.tsv \
    -O gwas_catalog_v1.0.2-associations_e${ENSEMBL}_r${YEAR}-${MONTH}-${DAY}.tsv
logging "File gwas_catalog_v1.0.2-associations_e${ENSEMBL}_r${YEAR}-${MONTH}-${DAY}.tsv saved."

wget -q ${RELEASE_URL}/gwas-catalog-download-studies-v1.0.3.txt \
    -O gwas-catalog-v1.0.3-studies-r${YEAR}-${MONTH}-${DAY}.tsv
logging "File gwas-catalog-v1.0.3-studies-r${YEAR}-${MONTH}-${DAY}.tsv saved."

wget -q ${RELEASE_URL}/gwas-catalog-download-ancestries-v1.0.3.txt \
    -O gwas-catalog-v1.0.3-ancestries-r${YEAR}-${MONTH}-${DAY}.tsv
logging "File gwas-catalog-v1.0.3-ancestries-r${YEAR}-${MONTH}-${DAY}.tsv saved."

wget -q ${BASE_URL}/summary_statistics/harmonised_list.txt -O harmonised_list-r${YEAR}-${MONTH}-${DAY}.txt
logging "File harmonised_list-r${YEAR}-${MONTH}-${DAY}.txt saved."

logging "Copying files to GCP..."
gsutil  -q cp file://$(pwd)/gwas_catalog_v1.0.2-associations_e${ENSEMBL}_r${YEAR}-${MONTH}-${DAY}.tsv ${GCP_TARGET}/
gsutil  -q cp file://$(pwd)/gwas-catalog-v1.0.3-studies-r${YEAR}-${MONTH}-${DAY}.tsv ${GCP_TARGET}/
gsutil  -q cp file://$(pwd)/gwas-catalog-v1.0.3-ancestries-r${YEAR}-${MONTH}-${DAY}.tsv ${GCP_TARGET}/
gsutil  -q cp file://$(pwd)/harmonised_list-r${YEAR}-${MONTH}-${DAY}.txt ${GCP_TARGET}/

logging "Done."
