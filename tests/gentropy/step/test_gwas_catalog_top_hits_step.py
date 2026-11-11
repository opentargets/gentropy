"""Test the GWAS Catalog top hits ingestion step."""

from __future__ import annotations

from pathlib import Path

import pyspark.sql.functions as f
import pytest

from gentropy.common.session import Session
from gentropy.dataset.study_locus import AnnotatedStudyLocus, StudyLocusQualityCheck
from gentropy.dataset.variant_index import VariantIndex
from gentropy.datasource.gwas_catalog.study_index import StudyIndexGWASCatalog
from gentropy.gwas_catalog_top_hits import GWASCatalogTopHitIngestionStep

ASSOCIATIONS = "tests/gentropy/data_samples/gwas_catalog_associations.tsv"


def _variant_index_of_hits(session: Session) -> VariantIndex:
    """A variant index holding the sample hits, so they map by rsId and risk allele.

    Args:
        session (Session): Session object.

    Returns:
        VariantIndex: One single-nucleotide variant per placed sample hit, with the risk allele as
            the alternative allele.
    """
    hits = (
        session.spark.read.csv(ASSOCIATIONS, sep="\t", header=True)
        .select(
            f.col("CHR_ID").alias("chromosome"),
            f.col("CHR_POS").cast("int").alias("position"),
            f.split(f.col("STRONGEST SNP-RISK ALLELE"), "-").getItem(1).alias("alt"),
            f.col("SNPS").alias("rsId"),
        )
        .filter(
            f.col("position").isNotNull()
            & f.col("chromosome").rlike("^[0-9XY]+$")
            & f.col("alt").rlike("^[ACGT]$")
            & f.col("rsId").startswith("rs")
        )
        .withColumn("ref", f.when(f.col("alt") == "A", "C").otherwise("A"))
        .dropDuplicates(["chromosome", "position"])
    )
    schema = VariantIndex.get_schema()
    variants = hits.select(
        f.concat_ws("_", "chromosome", "position", "ref", "alt").alias("variantId"),
        "chromosome",
        "position",
        f.col("ref").alias("referenceAllele"),
        f.col("alt").alias("alternateAllele"),
        f.array("rsId").alias("rsIds"),
    )
    return VariantIndex(
        _df=variants.select(
            *(
                f.col(field.name)
                if field.name in variants.columns
                else f.lit(None).cast(field.dataType).alias(field.name)
                for field in schema.fields
            )
        ),
        _schema=schema,
    )


@pytest.mark.step_test
def test_gwas_catalog_top_hits_step(session: Session, tmp_path: Path) -> None:
    """The step writes the split study index and one point study locus per placed hit."""
    variant_index_path = str(tmp_path / "variant_index")
    _variant_index_of_hits(session).df.write.parquet(variant_index_path)
    studies_path = str(tmp_path / "studies")
    loci_path = str(tmp_path / "study_locus")

    GWASCatalogTopHitIngestionStep(
        session=session,
        catalog_study_files=["tests/gentropy/data_samples/gwas_catalog_studies.tsv"],
        catalog_ancestry_files=[
            "tests/gentropy/data_samples/gwas_catalog_ancestries.tsv"
        ],
        catalog_associations_file=ASSOCIATIONS,
        variant_annotation_path=variant_index_path,
        catalog_studies_out=studies_path,
        catalog_associations_out=loci_path,
    )

    assert StudyIndexGWASCatalog.from_parquet(session, studies_path).df.count() > 0
    loci = AnnotatedStudyLocus.from_parquet(session, loci_path).df
    assert loci.count() > 0
    assert (
        loci.filter(
            (f.col("locusStart") != f.col("locusEnd"))
            | ~f.array_contains("qualityControls", StudyLocusQualityCheck.TOP_HIT.value)
            | (f.size("locus") != 1)
        ).count()
        == 0
    )
    assert loci.select("id").distinct().count() == loci.count()
