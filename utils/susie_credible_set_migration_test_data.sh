#!/usr/bin/env bash
# Test data for utils/susie_credible_set_migration.sh: a few credible sets of one partition of
# each source's QC output, the study index, and the harmonised summary statistics of their studies,
# copied under gs://ot_orchestration/tests/susie_credible_set_migration/<source>/.
# Temporary: delete with the migration scripts.
#
# Usage: utils/susie_credible_set_migration_test_data.sh [credible sets per source, default 4]
# Requires gcloud and uv.

set -euo pipefail

readonly N_SETS="${1:-4}"
readonly TEST_ROOT="gs://ot_orchestration/tests/susie_credible_set_migration"
readonly WORKDIR="$(mktemp -d)"
trap 'rm -rf "${WORKDIR}"' EXIT

# source  credible sets  study index  summary statistics root  study directory prefix
readonly SOURCES=(
    "ukb_ppp_eur gs://ukb_ppp_eur_data/credible_set_clean/20250129 gs://ukb_ppp_eur_data/study_index gs://ukb_ppp_eur_inputs/harmonised_summary_statistics studyId="
    "gwas_catalog gs://gwas_catalog_sumstats_susie/credible_set_clean/20260901 gs://gwas_catalog_sumstats_susie/study_index gs://gwas_catalog_inputs/harmonised_summary_statistics -"
)

for entry in "${SOURCES[@]}"; do
    read -r name credible_sets study_index sumstats prefix <<<"${entry}"
    [[ "${prefix}" == "-" ]] && prefix=""
    target="${TEST_ROOT}/${name}"
    local_dir="${WORKDIR}/${name}"
    mkdir -p "${local_dir}"
    echo "== ${name}"

    partition=$(gcloud storage ls "${credible_sets}/*.parquet" | head -1)
    gcloud storage cp "${partition}" "${local_dir}/partition.parquet"

    # A few credible sets with several members, and one with a single member.
    uv run --quiet --with pyarrow python - "${local_dir}" "${N_SETS}" <<'EOF'
import sys

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

local_dir, n_sets = sys.argv[1], int(sys.argv[2])
table = pq.read_table(f"{local_dir}/partition.parquet").sort_by("studyLocusId")
sizes = pc.list_value_length(table.column("locus"))
several = table.filter(pc.greater(sizes, 1)).slice(0, n_sets - 1)
single = table.filter(pc.equal(sizes, 1)).slice(0, 1)
subset = pa.concat_tables([several, single])
pq.write_table(subset, f"{local_dir}/credible_sets.parquet")
with open(f"{local_dir}/studies.txt", "w") as out:
    out.write("\n".join(sorted(set(subset.column("studyId").to_pylist()))) + "\n")
print(f"{subset.num_rows} credible sets, {sum(pc.list_value_length(subset.column('locus')).to_pylist())} members")
EOF

    gcloud storage rm -r --quiet "${target}" 2>/dev/null || true
    gcloud storage cp "${local_dir}/credible_sets.parquet" "${target}/credible_set_clean/part-00000.parquet"
    gcloud storage cp -r "${study_index}/*" "${target}/study_index/"
    while read -r study_id; do
        echo "summary statistics of ${study_id}"
        gcloud storage cp -r "${sumstats}/${prefix}${study_id}" "${target}/harmonised_summary_statistics/"
    done <"${local_dir}/studies.txt"
done
