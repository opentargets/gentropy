#!/usr/bin/env bash
# One-off conversion of released SuSiE-inf credible sets to CredibleSet + CredibleSetVariant.
# Temporary: delete with utils/susie_credible_set_migration.py once the data is converted.
#
# Usage: utils/susie_credible_set_migration.sh <command> [source]
#   sync                 copy the PySpark job to ${SCRIPT_URI}
#   create               create the Dataproc cluster ${CLUSTER} (no init actions: PySpark only)
#   submit <source>      run the conversion for one source on the cluster
#   delete               delete the cluster
#   run <source>         sync, create, submit, and delete the cluster even when the job fails
# Sources: test_ukb_ppp_eur, test_gwas_catalog, ukb_ppp_eur, gwas_catalog.
#
# Override with environment variables: PROJECT, REGION, CLUSTER, NUM_WORKERS, WORKER_MACHINE_TYPE.

set -euo pipefail

readonly PROJECT="${PROJECT:-open-targets-genetics-dev}"
readonly REGION="${REGION:-europe-west1}"
readonly CLUSTER="${CLUSTER:-susie-credible-set-migration}"
readonly NUM_WORKERS="${NUM_WORKERS:-2}"
readonly WORKER_MACHINE_TYPE="${WORKER_MACHINE_TYPE:-n1-highmem-8}"
readonly IMAGE_VERSION="2.2-debian12"
readonly SCRIPT_URI="gs://gwas_catalog_sumstats_susie/migration/susie_credible_set_migration.py"
readonly LOCAL_SCRIPT="$(dirname "$0")/susie_credible_set_migration.py"
readonly TEST_ROOT="gs://ot_orchestration/tests/susie_credible_set_migration"

# Paths per source: credible sets (QC output), study index, one study's summary statistics
# ({studyId} is filled in), output root, partitions of credible_set_variant.
function source_args() {
    case "$1" in
        test_ukb_ppp_eur)
            echo "--credible-sets=${TEST_ROOT}/ukb_ppp_eur/credible_set_clean" \
                "--study-index=${TEST_ROOT}/ukb_ppp_eur/study_index" \
                "--summary-statistics=${TEST_ROOT}/ukb_ppp_eur/harmonised_summary_statistics/studyId={studyId}" \
                "--output=${TEST_ROOT}/ukb_ppp_eur/output" \
                "--partitions=4"
            ;;
        test_gwas_catalog)
            echo "--credible-sets=${TEST_ROOT}/gwas_catalog/credible_set_clean" \
                "--study-index=${TEST_ROOT}/gwas_catalog/study_index" \
                "--summary-statistics=${TEST_ROOT}/gwas_catalog/harmonised_summary_statistics/{studyId}" \
                "--output=${TEST_ROOT}/gwas_catalog/output" \
                "--partitions=4"
            ;;
        ukb_ppp_eur)
            echo "--credible-sets=gs://ukb_ppp_eur_data/credible_set_clean/20250129" \
                "--study-index=gs://ukb_ppp_eur_data/study_index" \
                "--summary-statistics=gs://ukb_ppp_eur_inputs/harmonised_summary_statistics/studyId={studyId}" \
                "--output=gs://ukb_ppp_eur_data/credible_set_migrated/20250129" \
                "--partitions=50"
            ;;
        gwas_catalog)
            echo "--credible-sets=gs://gwas_catalog_sumstats_susie/credible_set_clean/20260901" \
                "--study-index=gs://gwas_catalog_sumstats_susie/study_index" \
                "--summary-statistics=gs://gwas_catalog_inputs/harmonised_summary_statistics/{studyId}" \
                "--output=gs://gwas_catalog_sumstats_susie/credible_set_migrated/20260901" \
                "--partitions=200"
            ;;
        *)
            echo "Unknown source: $1" >&2
            exit 1
            ;;
    esac
}

function sync() {
    gcloud storage cp "${LOCAL_SCRIPT}" "${SCRIPT_URI}"
}

function create() {
    gcloud dataproc clusters create "${CLUSTER}" \
        --project="${PROJECT}" \
        --region="${REGION}" \
        --image-version="${IMAGE_VERSION}" \
        --master-machine-type=n1-standard-8 \
        --master-boot-disk-size=200GB \
        --num-workers="${NUM_WORKERS}" \
        --worker-machine-type="${WORKER_MACHINE_TYPE}" \
        --worker-boot-disk-size=200GB \
        --max-idle=30m \
        --properties="spark:spark.sql.adaptive.enabled=true,spark:spark.sql.parquet.compression.codec=zstd"
}

function submit() {
    # shellcheck disable=SC2046
    gcloud dataproc jobs submit pyspark "${SCRIPT_URI}" \
        --project="${PROJECT}" \
        --region="${REGION}" \
        --cluster="${CLUSTER}" \
        -- $(source_args "$1")
}

function delete() {
    gcloud dataproc clusters delete "${CLUSTER}" --project="${PROJECT}" --region="${REGION}" --quiet
}

function run() {
    sync
    create
    trap delete EXIT
    submit "$1"
}

case "${1:-}" in
    sync | create | delete) "$1" ;;
    submit | run) "$1" "${2:?source required}" ;;
    *)
        sed -n '2,13p' "$0"
        exit 1
        ;;
esac
