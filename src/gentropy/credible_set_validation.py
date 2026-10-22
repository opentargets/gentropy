"""Step to validate credible sets against the study index."""

from __future__ import annotations

from functools import reduce

from gentropy.common.session import Session
from gentropy.dataset.credible_set import CredibleSet, CredibleSetVariant
from gentropy.dataset.study_index import StudyIndex
from gentropy.dataset.target_index import TargetIndex


class CredibleSetValidationStep:
    """Credible set validation step.

    Flags credible sets with quality issues, assigns their confidence, and splits them, with their
    variants, into valid and invalid datasets. The checks are those of the study locus validation
    step that ran on the previous study locus schema, in the same order:

    1. `qc_mhc_region`, `validate_chromosome_label`: lead variant in the MHC region, invalid chromosome.
    2. `validate_study`, `annotate_study_type`: flagged or missing study, curated top hit of a study
       with summary statistics; study type from the study index.
    3. `qc_redundant_top_hits_from_pics`, `qc_explained_by_susie`: curated top hits of studies with
       PICS credible sets from summary statistics, credible sets in regions explained by SuSiE.
    4. `qc_abnormal_pips`: sum of the posterior probabilities outside [0.95, 1.0001].
    5. `assign_confidence`, `flag_trans_qtls`.
    6. The credible sets with any of `invalid_qc_reasons` are invalid. Duplicated identifiers are
       then flagged among the valid ones, and replication is assessed last, on those that remain.

    Credible sets store only their 95% credible set, so there is no credible interval to filter.
    """

    def __init__(
        self,
        session: Session,
        credible_set_path: list[str],
        credible_set_variant_path: list[str],
        study_index_path: str,
        target_index_path: str,
        disease_index_path: str,
        valid_credible_set_path: str,
        valid_credible_set_variant_path: str,
        invalid_credible_set_path: str,
        invalid_credible_set_variant_path: str,
        trans_qtl_threshold: int,
        invalid_qc_reasons: list[str] | None = None,
    ) -> None:
        """Run the step.

        Args:
            session (Session): Session object.
            credible_set_path (list[str]): Paths to the credible sets, one per source.
            credible_set_variant_path (list[str]): Paths to their credible set variants.
            study_index_path (str): Path to the validated study index.
            target_index_path (str): Path to the target index.
            disease_index_path (str): Path to the platform disease index, used to tell measurements from diseases in the replication check.
            valid_credible_set_path (str): Output path of the valid credible sets.
            valid_credible_set_variant_path (str): Output path of their variants.
            invalid_credible_set_path (str): Output path of the invalid credible sets.
            invalid_credible_set_variant_path (str): Output path of their variants.
            trans_qtl_threshold (int): Genomic distance above which a QTL is considered trans.
            invalid_qc_reasons (list[str] | None): Names of the `StudyLocusQualityCheck` flags that make a credible set invalid (e.g. ['SUBSIGNIFICANT_FLAG']).
        """
        invalid_qc_reasons = list(invalid_qc_reasons) if invalid_qc_reasons else []
        # The study index is consumed by four of the checks, so it is cached:
        study_index = StudyIndex.from_parquet(session, study_index_path).persist()
        target_index = TargetIndex.from_parquet(session, target_index_path)
        credible_sets = CredibleSet(
            reduce(
                lambda a, b: a.unionByName(b),
                (CredibleSet.from_parquet(session, p).df for p in credible_set_path),
            )
        )
        variants = CredibleSetVariant(
            reduce(
                lambda a, b: a.unionByName(b),
                (
                    CredibleSetVariant.from_parquet(session, p).df
                    for p in credible_set_variant_path
                ),
            )
        ).persist()

        flagged = (
            credible_sets.qc_mhc_region()
            .validate_chromosome_label()
            .validate_study(study_index)
            .annotate_study_type(study_index)
            .qc_redundant_top_hits_from_pics()
            .qc_explained_by_susie(variants)
            .qc_abnormal_pips(
                variants, sum_pips_lower_threshold=0.95, sum_pips_upper_threshold=1.0001
            )
            .assign_confidence()
            .flag_trans_qtls(variants, study_index, target_index, trans_qtl_threshold)
            .persist()  # Feeds both outputs.
        )
        result = flagged.valid_rows(invalid_qc_reasons)

        # Identifiers are unique within a source, but the input is a union of several, so
        # uniqueness can only be checked here. It is checked on the credible sets that survived the
        # other flags: a curated top hit collides with the PICS credible set of the same study and
        # lead, and the top hit, the smaller of the two, would be kept although it is already
        # invalid under TOP_HIT_AND_SUMMARY_STATS, losing both.
        deduplicated = result.valid.validate_unique_id(variants).persist()
        unique = deduplicated.valid_rows(invalid_qc_reasons)

        # Replication is assessed last, once every invalid credible set is removed: an invalid
        # credible set is no evidence of replication, and duplicates would count twice.
        replicated = unique.valid.qc_replication(
            study_index,
            session.load_data(
                disease_index_path,
                "parquet",
                schema="id STRING, therapeuticAreas ARRAY<STRING>",
            ),
        ).persist()
        invalid = CredibleSet(
            result.invalid.df.unionByName(unique.invalid.df)
        ).persist()

        for dataset, cs_path, variant_path in (
            (replicated, valid_credible_set_path, valid_credible_set_variant_path),
            (invalid, invalid_credible_set_path, invalid_credible_set_variant_path),
        ):
            (
                dataset.df.repartition(
                    session.output_partitions, "studyId", "chromosome"
                )
                .sortWithinPartitions("chromosome", "leadVariantId")
                .write.mode(session.write_mode)
                .parquet(cs_path)
            )
            (
                variants.df.join(
                    dataset.df.select("id").withColumnRenamed("id", "credibleSetId"),
                    on="credibleSetId",
                    how="left_semi",
                )
                .repartition(session.output_partitions, "credibleSetId")
                .sortWithinPartitions("credibleSetId", "variantId")
                .write.mode(session.write_mode)
                .parquet(variant_path)
            )

        for cached in (
            invalid,
            replicated,
            deduplicated,
            flagged,
            variants,
            study_index,
        ):
            cached.df.unpersist()
