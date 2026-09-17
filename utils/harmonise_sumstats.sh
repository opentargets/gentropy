#!/bin/bash
# Script for running harmonisation and qc steps by the google batch job
# Requirements:
# 1. Gentropy
# 2. gcloud
# 3. gzip

# set -x

readonly RAW_FILE=$1
readonly HARMONISED_FILE=$2
readonly QC_FILE=$3
readonly QC_THRESHOLD=$4
export HYDRA_FULL_ERROR=1

logging() {
    log_prompt="[$(date "+%Y.%m.%d %H:%M")]"
    echo "${log_prompt} $@" | tee -a ${LOCAL_LOG_FILE}
}

# NOTE: Harmonised path contains ${output_path}/harmonised_sumstats/${study_id}
HARMONISATION_DIR=$(dirname $HARMONISED_FILE)
OUTPUT_PATH=$(dirname $HARMONISATION_DIR)
STUDY_ID=$(basename $HARMONISED_FILE)
LOCAL_LOG_FILE="harmonisation.log"
LOCAL_SUMMARY_FILE=harmonisation.csv
RAW_LOCAL_FILE=$(basename $RAW_FILE)
UNZIPPED_RAW_LOCAL_FILE="${RAW_LOCAL_FILE%.*}"

# Make sure we start with clean setup
if [ -f ${LOCAL_SUMMARY_FILE} ]; then
    rm -rf ${LOCAL_SUMMARY_FILE}
fi
echo "study,harmonisationExitCode,qcExitCode,rawSumstatFile,rawSumstatFileSize,rawUnzippedSumstatFileSize" >$LOCAL_SUMMARY_FILE

if [ -f ${LOCAL_LOG_FILE} ]; then
    rm -rf ${LOCAL_LOG_FILE}
fi

logging "Copying raw summary statistics from ${RAW_FILE} to ${RAW_LOCAL_FILE}"
gcloud storage cp $RAW_FILE $RAW_LOCAL_FILE

RAW_FILE_SIZE=$(du -sh ${RAW_LOCAL_FILE} | cut -f1)
logging "Raw file size ${RAW_FILE_SIZE}"

logging "Unzipping ${RAW_LOCAL_FILE} to ${UNZIPPED_RAW_LOCAL_FILE}"
gzip -d $RAW_LOCAL_FILE

UNZIPPED_FILE_SIZE=$(du -sh ${UNZIPPED_RAW_LOCAL_FILE} | cut -f1)
logging "Unzipped file size ${UNZIPPED_FILE_SIZE}"

logging "Running harmonisation on ${UNZIPPED_RAW_LOCAL_FILE} file"
gentropy step=gwas_catalog_sumstat_preprocess \
    step.raw_sumstats_path=$UNZIPPED_RAW_LOCAL_FILE \
    step.out_sumstats_path=$HARMONISED_FILE \
    step.session.write_mode=overwrite \
    step.session.add_gcs_connector=true \
    +step.session.extended_spark_conf="{spark.dynamicAllocation.enabled:false}" \
    +step.session.extended_spark_conf="{spark.driver.memory:16g}" \
    +step.session.extended_spark_conf="{spark.kryoserializer.buffer.max:500m}" \
    +step.session.extended_spark_conf="{spark.driver.maxResultSize:5g}" >>${LOCAL_LOG_FILE} 2>&1
# NOTE: can not use tee to redirect, otherwise the exit code will always be 0
HARMONISATION_EXIT_CODE=$?
logging "Harmonisation exit code: ${HARMONISATION_EXIT_CODE}"

logging "Running qc on ${HARMONISED_FILE} file"
gentropy step=summary_statistics_qc \
    step.gwas_path=$HARMONISED_FILE \
    step.output_path=$QC_FILE \
    step.pval_threshold=$QC_THRESHOLD \
    step.session.write_mode=overwrite \
    step.session.add_gcs_connector=true \
    +step.session.extended_spark_conf="{spark.dynamicAllocation.enabled:false}" \
    +step.session.extended_spark_conf="{spark.driver.memory:16g}" \
    +step.session.extended_spark_conf="{spark.kryoserializer.buffer.max:500m}" \
    +step.session.extended_spark_conf="{spark.driver.maxResultSize:5g}" >>${LOCAL_LOG_FILE} 2>&1
QC_EXIT_CODE=$?
logging "QC exit code: ${QC_EXIT_CODE}"

echo "$STUDY_ID,$HARMONISATION_EXIT_CODE,$QC_EXIT_CODE,$RAW_FILE,$RAW_FILE_SIZE,$UNZIPPED_FILE_SIZE" >>$LOCAL_SUMMARY_FILE

# shellcheck disable=SC2329
clean_up() {
    # ensure the logs from the job and summary of harmonisation & qc are preserved (latest are overwritten and dated are maintained)
    DATE=$(date "+%Y%m%d%H%M")
    REMOTE_LOG_FILE="${OUTPUT_PATH}/harmonisation_summary/${STUDY_ID}/${DATE}/harmonisation.log"
    LATEST_REMOTE_LOG_FILE="${OUTPUT_PATH}/harmonisation_summary/${STUDY_ID}/latest/harmonisation.log"
    REMOTE_SUMMARY_FILE="${OUTPUT_PATH}/harmonisation_summary/${STUDY_ID}/${DATE}/harmonisation.csv"
    LATEST_REMOTE_SUMMARY_FILE="${OUTPUT_PATH}/harmonisation_summary/${STUDY_ID}/latest/harmonisation.csv"

    gcloud storage cp ${LOCAL_LOG_FILE} ${REMOTE_LOG_FILE}
    gcloud storage cp ${LOCAL_LOG_FILE} ${LATEST_REMOTE_LOG_FILE}

    gcloud storage cp ${LOCAL_SUMMARY_FILE} ${REMOTE_SUMMARY_FILE}
    gcloud storage cp ${LOCAL_SUMMARY_FILE} ${LATEST_REMOTE_SUMMARY_FILE}

}

trap clean_up EXIT

# exit with a non-zero exit code fist, otherwise 0
exit $HARMONISATION_EXIT_CODE
