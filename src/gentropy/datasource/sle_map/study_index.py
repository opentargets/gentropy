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

    `discoverySamples` holds the donors of the study's cell type by ancestry, from the donor
    counts of SLE-map. When a gene was fitted on fewer donors than its cell type has, the sample
    size is split over the ancestries of the cell type in proportion to their counts, rounded by
    largest remainder so the parts sum to the sample size, and `initialSampleSize` says so.

    An example row of the donor counts (`donor_counts_by_cell_type_and_ancestry.tsv`):

    | cell_type | ancestry | donor_count |
    |---|---|---|
    | CM_CD8_T_cells | AFR | 49 |
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

    # Ancestry codes of the donor counts, as labels of `gwas_population_2_LD_panel_map.json`.
    ANCESTRIES: tuple[tuple[str, str], ...] = (
        ("AFR", "African"),
        ("AMR", "Admixed American"),
        ("EAS", "East Asian"),
        ("EUR", "European"),
        ("SAS", "South Asian"),
        ("Unassigned", "NR"),
    )

    CONSTANTS = {
        "projectId": SleMapFinemapping.PROJECT_ID,
        "studyType": StudyType.SCEQTL.value,
        "hasSumstats": False,
        "publicationTitle": "Single-cell multi-ancestry regulatory map of systemic lupus erythematosus",
        "publicationFirstAuthor": "Jang H",
        "publicationDate": "2026-09-21",
        "publicationJournal": "bioRxiv",
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
    def read_donor_counts(
        cls: type[SleMapStudyIndex], session: Session, donor_counts_path: str
    ) -> dict[str, list[tuple[int, str]]]:
        """Read the donors of every cell type by ancestry.

        Args:
            session (Session): Session object.
            donor_counts_path (str): Path to `donor_counts_by_cell_type_and_ancestry.tsv`.

        Returns:
            dict[str, list[tuple[int, str]]]: Donor count and ancestry label by cell type, in the
                order of `ANCESTRIES`.

        Raises:
            ValueError: If an ancestry code is not in `ANCESTRIES`.
        """
        rows = (
            session.spark.read.option("delimiter", "\t")
            .option("header", True)
            .csv(donor_counts_path)
            .collect()
        )
        labels = dict(cls.ANCESTRIES)
        unknown = {r.ancestry for r in rows} - set(labels)
        if unknown:
            raise ValueError(f"Ancestries without a label: {sorted(unknown)}")
        order = [code for code, _ in cls.ANCESTRIES]
        counts: dict[str, list[tuple[int, str]]] = {}
        for r in sorted(rows, key=lambda r: order.index(r.ancestry)):
            counts.setdefault(r.cell_type, []).append(
                (int(r.donor_count), labels[r.ancestry])
            )
        return counts

    @staticmethod
    def split_by_ancestry(
        n: int, donors: list[tuple[int, str]]
    ) -> list[tuple[int, str]]:
        """Split a sample size over the ancestries of the donors, by largest remainder.

        Args:
            n (int): Sample size.
            donors (list[tuple[int, str]]): Donor count and ancestry.

        Returns:
            list[tuple[int, str]]: Sample size and ancestry, summing to `n`; the donor counts
                themselves when `n` is their total.

        Examples:
            >>> donors = [(49, "African"), (1, "Admixed American"), (9, "East Asian"), (48, "European"), (37, "South Asian"), (4, "NR")]
            >>> SleMapStudyIndex.split_by_ancestry(148, donors) == donors
            True
            >>> SleMapStudyIndex.split_by_ancestry(100, donors)
            [(33, 'African'), (1, 'Admixed American'), (6, 'East Asian'), (32, 'European'), (25, 'South Asian'), (3, 'NR')]
        """
        total = sum(count for count, _ in donors)
        shares = [(label, n * count / total) for count, label in donors]
        sizes = {label: math.floor(share) for label, share in shares}
        by_remainder = sorted(shares, key=lambda x: (-(x[1] - math.floor(x[1])), x[0]))
        for label, _ in by_remainder[: n - sum(sizes.values())]:
            sizes[label] += 1
        return [(sizes[label], label) for _, label in donors]

    @staticmethod
    def initial_sample_size(n: int, donors: list[tuple[int, str]]) -> str:
        """Describe the sample size and, if it was split, the donors it was split over.

        Args:
            n (int): Sample size.
            donors (list[tuple[int, str]]): Donor count and ancestry of the cell type.

        Returns:
            str: The description.

        Examples:
            >>> donors = [(49, "African"), (1, "Admixed American"), (9, "East Asian"), (48, "European"), (37, "South Asian"), (4, "NR")]
            >>> SleMapStudyIndex.initial_sample_size(148, donors)
            '148 individuals (African 49, Admixed American 1, East Asian 9, European 48, South Asian 37, NR 4)'
            >>> SleMapStudyIndex.initial_sample_size(100, donors)
            '100 individuals; split over the ancestries of the 148 donors of the cell type (African 49, Admixed American 1, East Asian 9, European 48, South Asian 37, NR 4)'
        """
        total = sum(count for count, _ in donors)
        listed = ", ".join(f"{label} {count}" for count, label in donors)
        if n == total:
            return f"{n} individuals ({listed})"
        return f"{n} individuals; split over the ancestries of the {total} donors of the cell type ({listed})"

    @classmethod
    def from_source(
        cls: type[SleMapStudyIndex],
        session: Session,
        diagnostics_path: str | list[str],
        donor_counts_path: str,
        credible_sets: CredibleSet,
    ) -> StudyIndex:
        """Build the study index of the studies with credible sets.

        Args:
            session (Session): Session object.
            diagnostics_path (str | list[str]): Path to `all_diagnostics.tsv`.
            donor_counts_path (str): Path to `donor_counts_by_cell_type_and_ancestry.tsv`.
            credible_sets (CredibleSet): The credible sets.

        Returns:
            StudyIndex: One study per cell type and gene with credible sets.

        Raises:
            ValueError: If a cell type has no donor counts, or a gene was fitted on more donors
                than its cell type has.
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
        donor_counts = cls.read_donor_counts(session, donor_counts_path)
        # Few distinct cell types and sample sizes: split them on the driver.
        sizes = [
            (r.cellType, r.nSamples)
            for r in studies.select("cellType", "nSamples").distinct().collect()
            if r.nSamples is not None
        ]
        missing = {cell_type for cell_type, _ in sizes} - set(donor_counts)
        if missing:
            raise ValueError(f"Cell types without donor counts: {sorted(missing)}")
        too_large = sorted(
            (cell_type, n)
            for cell_type, n in sizes
            if n > sum(count for count, _ in donor_counts[cell_type])
        )
        if too_large:
            raise ValueError(
                f"Sample sizes above the donors of the cell type: {too_large}"
            )
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
                (
                    cell_type,
                    n,
                    cls.split_by_ancestry(n, donor_counts[cell_type]),
                    cls.initial_sample_size(n, donor_counts[cell_type]),
                )
                for cell_type, n in sizes
            ],
            t.StructType(
                [
                    t.StructField("cellType", t.StringType(), False),
                    t.StructField("nSamples", t.IntegerType(), False),
                    t.StructField("discoverySamples", samples_schema, True),
                    t.StructField("initialSampleSize", t.StringType(), True),
                ]
            ),
        )
        return StudyIndex(
            _df=studies.join(
                f.broadcast(samples), on=["cellType", "nSamples"], how="left"
            )
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
