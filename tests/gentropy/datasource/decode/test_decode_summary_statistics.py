"""Test summary statistics."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql import Row
from pyspark.sql import functions as f
from pyspark.sql import types as t

from gentropy.common.session import Session
from gentropy.dataset.study_index import ProteinQuantitativeTraitLocusStudyIndex
from gentropy.dataset.summary_statistics import SummaryStatistics
from gentropy.datasource.decode.summary_statistics import (
    deCODEHarmonisationConfig,
    deCODESummaryStatistics,
)


def assert_sumstat_equal(l1: list[Row], l2: list[Row]) -> bool:
    """Compare two lists of Rows for equality, ignoring order."""
    sorted_l1 = sorted(l1, key=lambda row: (row.studyId, row.variantId))
    sorted_l2 = sorted(l2, key=lambda row: (row.studyId, row.variantId))
    i = 0
    for row1, row2 in zip(sorted_l1, sorted_l2, strict=True):
        for field in SummaryStatistics.get_schema().fieldNames():
            if isinstance(row1[field], float) and isinstance(row2[field], float):
                cmp = row1[field] - row2[field] == pytest.approx(0, abs=1e5)
            else:
                cmp = row1[field] == row2[field]
            assert cmp, (
                f"Row: {i}: Field '{field}' does not match: {row1[field]} != {row2[field]}"
            )
        i += 1
    return True


class TestdeCODESummaryStatistics:
    """Test methods of deCODESummaryStatistics."""

    raw_data = [
        Row(
            # Retained variant, but flipped due to variant direction
            Chrom="chr1",
            Pos=1111,
            Name="chr1:1111:C:T",
            rsids=None,
            effectAllele="C",  # Expected alt = C
            otherAllele="T",  # Expected ref = T
            Beta=-0.0077,  # Expected beta is 0.0077
            Pval=0.7945,
            minus_log10_pval=0.1,
            SE=0.012345,
            N=35678,
            impMAF=0.07662,
            studyId="deCODE-proteomics-smp_Proteomics_SMP_PC0_10000_2_GENE1_PROTEIN1_00000001",
        ),
        Row(
            # Row filtered out due to low N (N = 20000)
            Chrom="chr1",
            Pos=152835,
            Name="chr1:152835:A:T",
            rsids="rs1446209547",
            effectAllele="A",
            otherAllele="T",
            Beta=-0.05,
            Pval=0.7945,
            minus_log10_pval=0.1,
            SE=0.438241,
            N=20000,
            impMAF=0.01,
            studyId="deCODE-proteomics-smp_Proteomics_SMP_PC0_10000_2_GENE1_PROTEIN1_00000001",
        ),
        Row(
            # Row filtered out due to low MAC (MAC = 6)
            Chrom="chr1",
            Pos=201430,
            Name="chr1:201430:TTC:T",
            rsids="rs1205330954",
            effectAllele="TTC",
            otherAllele="T",
            Beta=-0.0077,
            Pval=0.7945,
            minus_log10_pval=0.1,
            SE=0.015984,
            N=30000,
            impMAF=0.00001,
            studyId="deCODE-proteomics-smp_Proteomics_SMP_PC0_10000_2_GENE1_PROTEIN1_00000001",
        ),
        Row(
            # Row retained, but EAF is 1 - MAF
            Chrom="chr1",
            Pos=455948,
            Name="chr1:455948:G:C",
            rsids="rs1363653182",
            effectAllele="G",
            otherAllele="C",
            Beta=0.1027,
            Pval=0.7945,
            minus_log10_pval=0.1,
            SE=0.012345,
            N=34567,
            impMAF=0.95,  # Expected EAF is 0.05
            studyId="deCODE-proteomics-smp_Proteomics_SMP_PC0_10000_2_GENE1_PROTEIN1_00000001",
        ),
        Row(
            # Row retained, missing from gnomAD
            Chrom="chr1",
            Pos=455949,
            Name="chr1:455949:G:C",
            rsids=None,
            effectAllele="G",
            otherAllele="C",
            Beta=0.1027,
            Pval=0.7945,
            minus_log10_pval=0.1,
            SE=0.012345,
            N=34567,
            impMAF=0.05,
            studyId="deCODE-proteomics-smp_Proteomics_SMP_PC0_10000_2_GENE1_PROTEIN1_00000001",
        ),
    ]

    vd_data = [
        Row(
            chromosome="1",
            rangeId=0,
            originalVariantId="1_1111_T_C",
            variantId="1_1111_C_T",
            direction=-1,
            originalAlleleFrequencies=[
                Row(populationName="nfe_adj", alleleFrequency=0.07662)
            ],
            strand=1,
        ),
        Row(
            chromosome="1",
            rangeId=0,
            originalVariantId="1_1111_T_C",
            variantId="1_1111_T_C",
            direction=1,
            originalAlleleFrequencies=[
                Row(populationName="nfe_adj", alleleFrequency=0.07662)
            ],
            strand=1,
        ),
        Row(
            chromosome="1",
            rangeId=15,
            originalVariantId="1_152835_A_T",
            variantId="1_152835_A_T",
            direction=1,
            originalAlleleFrequencies=[
                Row(populationName="nfe_adj", alleleFrequency=0.07662)
            ],
            strand=1,
        ),
        Row(
            chromosome="1",
            rangeId=20,
            originalVariantId="1_201430_TTC_T",
            variantId="1_201430_TTC_T",
            direction=1,
            originalAlleleFrequencies=[
                Row(populationName="nfe_adj", alleleFrequency=0.07662)
            ],
            strand=1,
        ),
        Row(
            chromosome="1",
            rangeId=45,
            originalVariantId="1_455948_G_C",
            variantId="1_455948_G_C",
            direction=1,
            originalAlleleFrequencies=[
                Row(populationName="nfe_adj", alleleFrequency=0.07662)
            ],
            strand=1,
        ),
    ]

    expected_harm_rows = [
        # This allele is flipped with regards to source (variantId + beta sign)
        Row(
            studyId="deCODE-proteomics-smp_Proteomics_SMP_PC0_10000-2_GENE1_PROTEIN1",
            variantId="1_1111_T_C",
            chromosome="1",
            position=1111,
            beta=0.0077,
            sampleSize=35678,
            pValueMantissa=7.943282,
            pValueExponent=-1,
            effectAlleleFrequencyFromSource=0.07662,
            standardError=0.012345,
        ),
        # This allele does not have anything changed, is found in variant direction
        Row(
            studyId="deCODE-proteomics-smp_Proteomics_SMP_PC0_10000-2_GENE1_PROTEIN1",
            variantId="1_455948_G_C",
            chromosome="1",
            position=455948,
            beta=0.1027,
            sampleSize=34567,
            pValueMantissa=7.943282,
            pValueExponent=-1,
            effectAlleleFrequencyFromSource=0.05,
            standardError=0.012345,
        ),
        # This allele does not have anything changed, is not found in variant direction
        Row(
            studyId="deCODE-proteomics-smp_Proteomics_SMP_PC0_10000-2_GENE1_PROTEIN1",
            variantId="1_455949_G_C",
            chromosome="1",
            position=455949,
            beta=0.1027,
            sampleSize=34567,
            pValueMantissa=7.943282,
            pValueExponent=-1,
            effectAlleleFrequencyFromSource=0.05,
            standardError=0.012345,
        ),
    ]

    @patch("gentropy.datasource.decode.summary_statistics.VariantDirection")
    def test_from_source(
        self, mock_variant_direction: MagicMock, session: Session
    ) -> None:
        """Test building summary statistics from source — filters, flips, and EAF inference."""
        raw = session.spark.createDataFrame(self.raw_data)
        vd_df = session.spark.createDataFrame(self.vd_data)
        vd_instance = MagicMock(df=vd_df)
        mock_variant_direction.return_value = vd_instance

        # Build a minimal pQTL study index DF with the required non-nullable columns
        source_study_id = (
            "deCODE-proteomics-smp_Proteomics_SMP_PC0_10000_2_GENE1_PROTEIN1_00000001"
        )
        targets_schema = t.ArrayType(
            t.StructType(
                [
                    t.StructField("geneSymbol", t.StringType(), nullable=True),
                    t.StructField("proteinId", t.StringType(), nullable=True),
                    t.StructField("geneId", t.StringType(), nullable=True),
                    t.StructField("proteinName", t.StringType(), nullable=True),
                ]
            ),
            containsNull=True,
        )
        si_schema = t.StructType(
            [
                t.StructField("studyId", t.StringType(), nullable=False),
                t.StructField("projectId", t.StringType(), nullable=False),
                t.StructField("studyType", t.StringType(), nullable=False),
                t.StructField("targetsFromSource", targets_schema, nullable=True),
            ]
        )
        si_df = session.spark.createDataFrame(
            [
                (
                    source_study_id,
                    "deCODE-proteomics-smp",
                    "pqtl",
                    [
                        Row(
                            geneSymbol="GENE1",
                            proteinId="P12345",
                            geneId=None,
                            proteinName="PROTEIN1",
                        )
                    ],
                )
            ],
            schema=si_schema,
        )
        mock_si = MagicMock()
        mock_si.df = si_df

        config = deCODEHarmonisationConfig(
            min_mac=10,
            min_sample_size=30000,
            flipping_window_size=10000,
        )
        result = deCODESummaryStatistics.from_source(
            raw_summary_statistics=raw,
            variant_direction=vd_instance,
            decode_study_index=mock_si,
            config=config,
        )
        harm, pqtl_si = result
        assert isinstance(harm, SummaryStatistics), (
            "first element should be SummaryStatistics"
        )
        assert isinstance(pqtl_si, ProteinQuantitativeTraitLocusStudyIndex), (
            "second element should be ProteinQuantitativeTraitLocusStudyIndex"
        )
        assert harm.df.count() == 3
        assert_sumstat_equal(harm.df.collect(), self.expected_harm_rows)

    def test_txtgz_to_parquet(self, tmp_path: Path, session: Session) -> None:
        """Test converting txt.gz summary statistics to parquet."""
        input_path = (
            tmp_path / "Proteomics_SMP_PC0_10000_2_GENE1_PROTEIN1_00000001.txt.gz"
        )
        output_path = tmp_path / "harmonised_sumstats"
        session.spark.createDataFrame(self.expected_harm_rows).toPandas().to_csv(
            input_path, sep="\t", compression="gzip"
        )
        assert input_path, "Parquet file should be created"
        deCODESummaryStatistics.txtgz_to_parquet(
            session, [input_path.as_posix()], output_path.as_posix(), n_threads=1
        )

    @pytest.mark.parametrize(
        "imp_maf,eur_af,expected_eaf",
        [
            # eur_af is null → use imp_maf directly
            (0.01, None, 0.01),
            # |eur_af - imp_maf| <= |eur_af - (1 - imp_maf)| → use imp_maf
            (0.01, 0.02, 0.01),
            # |eur_af - (1 - imp_maf)| < |eur_af - imp_maf| → use 1 - imp_maf
            (0.01, 0.60, 0.99),
        ],
    )
    def test_infer_allele_frequency(
        self,
        session: Session,
        imp_maf: float,
        eur_af: float | None,
        expected_eaf: float,
    ) -> None:
        """_infer_allele_frequency should pick imp_maf or 1-imp_maf based on proximity to EUR_AF."""
        df = session.spark.createDataFrame(
            [(imp_maf, eur_af)], "impMAF DOUBLE, EUR_AF DOUBLE"
        )
        row = df.select(
            deCODESummaryStatistics._infer_allele_frequency(
                f.col("impMAF"), f.col("EUR_AF")
            )
        ).collect()[0]
        assert row.effectAlleleFrequencyFromSource == pytest.approx(
            expected_eaf, abs=1e-6
        )
