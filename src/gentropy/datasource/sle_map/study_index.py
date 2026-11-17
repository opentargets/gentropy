"""Study index for the SLE-map single-cell eQTLs."""

from __future__ import annotations

import math
from dataclasses import dataclass
from itertools import chain

import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql import Column

from gentropy.common.session import Session
from gentropy.dataset.credible_set import CredibleSet
from gentropy.dataset.study_index import StudyIndex, StudyType
from gentropy.datasource.sle_map.finemapping import SleMapFinemapping


@dataclass
class SleMapStudyIndex:
    """Study index for the SLE-map single-cell eQTLs.

    One study per cell type and gene with at least one credible set. The sample size is the
    number of donors of the cell type and gene fitted (`n_samples` of the diagnostics).

    SLE-map gives the ancestries of the whole cohort only, not of the donors of each cell type
    and gene. `discoverySamples` splits each study's sample size over the ancestries of the cohort
    in proportion to their share of it, rounded by largest remainder so the parts sum to the
    sample size, and `initialSampleSize` says so.
    """

    # (cell type, Cell Ontology term). All cells together are peripheral blood mononuclear cells.
    CELL_TYPES: tuple[tuple[str, str], ...] = (
        ("Allcells", "CL_2000001"),
        ("CD56Bright_NK_cells", "CL_0000938"),
        ("CD56Dim_NK_cells", "CL_0000939"),
        ("CM_CD4_T_cells", "CL_0000904"),
        ("CM_CD8_T_cells", "CL_0000907"),
        ("Classical_Monocytes", "CL_0000576"),
        ("Cytotoxic_CD4_T_cells", "CL_0000934"),
        ("DN_T_cells", "CL_0002489"),
        ("EM_CD4_T_cells", "CL_0000905"),
        ("EM_CD8_T_cells", "CL_0000913"),
        ("Memory_B_cells", "CL_0000787"),
        ("Naive_B_cells", "CL_0000788"),
        ("Naive_CD4_T_cells", "CL_0000895"),
        ("Naive_CD8_T_cells", "CL_0000900"),
        ("Regulatory_CD4_T_cells", "CL_0000792"),
        ("TEMRA", "CL_4030002"),
    )

    # The donors of the cohort by ancestry, as labels of `gwas_population_2_LD_panel_map.json`.
    COHORT_ANCESTRIES: tuple[tuple[str, int], ...] = (
        ("African", 102),
        ("Admixed American", 7),
        ("East Asian", 22),
        ("European", 81),
        ("South Asian", 62),
        ("NR", 7),
    )

    CONSTANTS = {
        "projectId": SleMapFinemapping.PROJECT_ID,
        "studyType": StudyType.SCEQTL.value,
        "hasSumstats": False,
        "publicationDate": "2026-09-18",
        "publicationJournal": "Preprint",
    }

    @classmethod
    def _biosample(cls: type[SleMapStudyIndex]) -> Column:
        """Cell Ontology term of `cellType`.

        Returns:
            Column: The term, null for an unknown cell type.
        """
        return f.create_map(*[f.lit(x) for x in chain(*cls.CELL_TYPES)])[
            f.col("cellType")
        ]

    @classmethod
    def split_by_ancestry(cls: type[SleMapStudyIndex], n: int) -> list[tuple[int, str]]:
        """Split a sample size over the ancestries of the cohort, by largest remainder.

        Args:
            n (int): Sample size.

        Returns:
            list[tuple[int, str]]: Sample size and ancestry, summing to `n`.

        Examples:
            >>> SleMapStudyIndex.split_by_ancestry(281)
            [(102, 'African'), (7, 'Admixed American'), (22, 'East Asian'), (81, 'European'), (62, 'South Asian'), (7, 'NR')]
            >>> sum(size for size, _ in SleMapStudyIndex.split_by_ancestry(148))
            148
        """
        total = sum(count for _, count in cls.COHORT_ANCESTRIES)
        shares = [(label, n * count / total) for label, count in cls.COHORT_ANCESTRIES]
        sizes = {label: math.floor(share) for label, share in shares}
        by_remainder = sorted(shares, key=lambda x: (-(x[1] - math.floor(x[1])), x[0]))
        for label, _ in by_remainder[: n - sum(sizes.values())]:
            sizes[label] += 1
        return [(sizes[label], label) for label, _ in cls.COHORT_ANCESTRIES]

    @classmethod
    def initial_sample_size(cls: type[SleMapStudyIndex], n: int) -> str:
        """Describe the sample size and how it was split over ancestries.

        Args:
            n (int): Sample size.

        Returns:
            str: The description.

        Examples:
            >>> SleMapStudyIndex.initial_sample_size(281)
            '281 individuals; split over the ancestries of the whole cohort of 281 (African 102, Admixed American 7, East Asian 22, European 81, South Asian 62, NR 7)'
        """
        total = sum(count for _, count in cls.COHORT_ANCESTRIES)
        cohort = ", ".join(f"{label} {count}" for label, count in cls.COHORT_ANCESTRIES)
        return f"{n} individuals; split over the ancestries of the whole cohort of {total} ({cohort})"

    @classmethod
    def from_source(
        cls: type[SleMapStudyIndex],
        session: Session,
        diagnostics_path: str | list[str],
        credible_sets: CredibleSet,
    ) -> StudyIndex:
        """Build the study index of the studies with credible sets.

        Args:
            session (Session): Session object.
            diagnostics_path (str | list[str]): Path to `all_diagnostics.tsv`.
            credible_sets (CredibleSet): The credible sets.

        Returns:
            StudyIndex: One study per cell type and gene with credible sets.
        """
        studies = (
            SleMapFinemapping.read_sample_sizes(session, diagnostics_path)
            .withColumn(
                "studyId",
                SleMapFinemapping.build_study_id(f.col("cellType"), f.col("geneId")),
            )
            .join(credible_sets.df.select("studyId").distinct(), on="studyId")
        )
        unknown = {
            r.cellType
            for r in studies.select("cellType").distinct().collect()
            if r.cellType not in dict(cls.CELL_TYPES)
        }
        if unknown:
            session.logger.warning(f"Cell types without annotation: {sorted(unknown)}")
        # Few distinct sample sizes: split them on the driver.
        sizes = [
            r.nSamples
            for r in studies.select("nSamples").distinct().collect()
            if r.nSamples is not None
        ]
        samples_schema = t.ArrayType(
            t.StructType(
                [
                    t.StructField("sampleSize", t.IntegerType(), True),
                    t.StructField("ancestry", t.StringType(), True),
                ]
            )
        )
        samples = session.spark.createDataFrame(
            [
                (n, cls.split_by_ancestry(n), cls.initial_sample_size(n))
                for n in sizes
            ],
            t.StructType(
                [
                    t.StructField("nSamples", t.IntegerType(), False),
                    t.StructField("discoverySamples", samples_schema, True),
                    t.StructField("initialSampleSize", t.StringType(), True),
                ]
            ),
        )
        return StudyIndex(
            _df=studies.join(f.broadcast(samples), on="nSamples", how="left")
            .select(
                "studyId",
                "geneId",
                f.col("geneId").alias("traitFromSource"),
                cls._biosample().alias("biosampleFromSourceId"),
                "nSamples",
                "initialSampleSize",
                "discoverySamples",
                f.array(f.lit("SLEmap")).alias("cohorts"),
                *[f.lit(value).alias(key) for key, value in cls.CONSTANTS.items()],
            )
            .withColumn(
                "ldPopulationStructure",
                StudyIndex.aggregate_and_map_ancestries(f.col("discoverySamples")),
            ),
            _schema=StudyIndex.get_schema(),
        )
