#!/usr/bin/env bash

# Function to get the most recent date:
get_most_recent(){
    cat $1 | perl -lane 'push @a, $_ if $_ =~ /^\d+$/; END {@a = sort { $a <=> $b} @a; print pop @a }'
}

# Function to return the path the to the most recent release:
get_release_url(){
    YEAR=$(curl -s --list-only ${BASE_URL}/releases/ | get_most_recent)
    MONTH=$(curl -s --list-only ${BASE_URL}/releases/${YEAR}/  | get_most_recent)
    DAY=$(curl -s --list-only ${BASE_URL}/releases/${YEAR}/${MONTH}/  | get_most_recent)
    echo $YEAR $MONTH $DAY
}

# Function to get the Ensembl and EFO version which used to ground GWAS data:
get_release_info(){
    curl -s https://www.ebi.ac.uk/gwas/api/search/stats | jq -r '"\(.ensemblbuild) \(.efoversion)"'
}

logging(){
    log_prompt="[$(date "+%Y.%m.%d %H:%M")]"
    echo "${log_prompt} $@" >> ${LOG_FILE}
}

# Resources:
export BASE_URL=ftp://ftp.ebi.ac.uk/pub/databases/gwas
export RELEASE_INFO_URL=https://www.ebi.ac.uk/gwas/api/search/stats
export GCP_TARGET=gs://gwas_catalog_data
export LOG_FILE=GWAS_Catalog_curated_data_update.log

export GWAS_CATALOG_STUDY_CURATION_URL=https://raw.githubusercontent.com/opentargets/curation/master/genetics/GWAS_Catalog_study_curation.tsv

ASSOCIATION_FILE=gwas-catalog-associations_ontology-annotated.tsv
PUBLISHED_STUDIES_FILE=gwas-catalog-download-studies.tsv
PUBLISHED_ANCESTRIES_FILE=gwas-catalog-download-ancestries.tsv
UNPUBLISHED_STUDIES_FILE=gwas-catalog-unpublished-studies.tsv
UNPUBLISHED_ANCESTRIES_FILE=gwas-catalog-unpublished-ancestries.tsv
HARMONISED_LIST_FILE=harmonised_list.txt

# Remove log file if exists:
if [ -f ${LOG_FILE} ]; then
    rm update_GWAS_Catalog_data.log
fi

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
wget -q ${RELEASE_URL}/gwas-catalog-associations_ontology-annotated.tsv -O ${ASSOCIATION_FILE}
logging "File ${ASSOCIATION_FILE} saved."

wget -q ${RELEASE_URL}/gwas-catalog-download-studies-v1.0.3.txt -O ${PUBLISHED_STUDIES_FILE}
logging "File ${PUBLISHED_STUDIES_FILE} saved."

wget -q ${RELEASE_URL}/gwas-catalog-unpublished-studies-v1.0.3.tsv -O ${UNPUBLISHED_STUDIES_FILE}
logging "File ${UNPUBLISHED_STUDIES_FILE} saved."

wget -q ${RELEASE_URL}/gwas-catalog-download-ancestries-v1.0.3.txt -O ${PUBLISHED_ANCESTRIES_FILE}
logging "File ${PUBLISHED_ANCESTRIES_FILE} saved."

wget -q ${RELEASE_URL}/gwas-catalog-unpublished-ancestries-v1.0.3.tsv -O ${UNPUBLISHED_ANCESTRIES_FILE}
logging "File ${UNPUBLISHED_ANCESTRIES_FILE} saved."

wget -q ${BASE_URL}/summary_statistics/harmonised_list.txt -O ${HARMONISED_LIST_FILE}
logging "File ${HARMONISED_LIST_FILE} saved."

wget -q ${GWAS_CATALOG_STUDY_CURATION_URL} -O GWAS_Catalog_study_curation.tsv
logging "In-house GWAS Catalog study curation file fetched from GitHub."

logging "Copying files to GCP..."
gsutil -mq cp file://$(pwd)/${ASSOCIATION_FILE} ${GCP_TARGET}/curated_inputs/
gsutil -mq cp file://$(pwd)/${PUBLISHED_STUDIES_FILE} ${GCP_TARGET}/curated_inputs/
gsutil -mq cp file://$(pwd)/${PUBLISHED_ANCESTRIES_FILE} ${GCP_TARGET}/curated_inputs/
gsutil -mq cp file://$(pwd)/${HARMONISED_LIST_FILE} ${GCP_TARGET}/curated_inputs/
gsutil -mq cp file://$(pwd)/${UNPUBLISHED_STUDIES_FILE} ${GCP_TARGET}/curated_inputs/
gsutil -mq cp file://$(pwd)/${UNPUBLISHED_ANCESTRIES_FILE} ${GCP_TARGET}/curated_inputs/
gsutil -mq cp file://$(pwd)/${GWAS_CATALOG_STUDY_CURATION_URL} ${GCP_TARGET}/manifests/

logging "Files successfully uploaded."
logging "Removing local files..."
rm ${ASSOCIATION_FILE} \
    ${PUBLISHED_STUDIES_FILE} \
    ${PUBLISHED_ANCESTRIES_FILE} \
    ${HARMONISED_LIST_FILE} \
    ${UNPUBLISHED_STUDIES_FILE} \
    ${UNPUBLISHED_ANCESTRIES_FILE}

# Uploading log file to GCP manifest folder:
logging "Uploading log file to GCP manifest folder..."
gsutil -mq cp file://$(pwd)/${LOG_FILE} ${GCP_TARGET}/manifests/
cat $LOG_FILE
