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
# Override with environment variables: PROJECT, REGION, CLUSTER, NUM_WORKERS, WORKER_MACHINE_TYPE,
# and RUN_TS (UTC, YYYYMMDDTHHMMSS) to reuse a snapshot of an earlier ukb_ppp_eur run.
#
# ukb_ppp_eur reads the released snapshot prod/20260526T131650/ and writes a new, self-contained
# snapshot prod/${RUN_TS}/: credible_set/ and credible_set_variant/ from the job, then study/,
# a copy of the released study index (never overwriting, verified by name, size and crc32c).

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
readonly UKB_BUCKET="gs://ukb_ppp_eur_data"
readonly UKB_RELEASED="${UKB_BUCKET}/prod/20260526T131650"
readonly RUN_TS="${RUN_TS:-$(date -u +%Y%m%dT%H%M%S)}"
readonly UKB_SNAPSHOT="${UKB_BUCKET}/prod/${RUN_TS}"

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
            echo "--credible-sets=${UKB_RELEASED}/credible_set_clean/20250129" \
                "--study-index=${UKB_RELEASED}/study_index" \
                "--summary-statistics=gs://ukb_ppp_eur_inputs/harmonised_summary_statistics/studyId={studyId}" \
                "--output=${UKB_SNAPSHOT}" \
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
    if [[ "$1" == "ukb_ppp_eur" ]]; then
        copy_study
        echo "snapshot: ${UKB_SNAPSHOT}"
    fi
}

# Copy the released study index into the new snapshot as study/, never overwriting, and check
# every object arrived with its size and crc32c. Folder markers (names ending in /, empty
# basename) are not copied by rsync, so they are left out of the comparison.
function copy_study() {
    local listing='value(name.basename(),size,crc32c_hash)'
    gcloud storage rsync --recursive --no-clobber "${UKB_RELEASED}/study_index" "${UKB_SNAPSHOT}/study"
    if diff <(gcloud storage objects list "${UKB_RELEASED}/study_index/**" --format="${listing}" | grep -v $'^\t' | sort) \
        <(gcloud storage objects list "${UKB_SNAPSHOT}/study/**" --format="${listing}" | grep -v $'^\t' | sort); then
        echo "study/ verified: ${UKB_SNAPSHOT}/study"
    else
        echo "study/ differs from ${UKB_RELEASED}/study_index" >&2
        exit 1
    fi
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
