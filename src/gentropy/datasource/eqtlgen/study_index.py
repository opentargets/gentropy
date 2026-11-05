"""Study index for the eQTLGen single-cell eQTLs and co-eQTLs."""

from __future__ import annotations

from dataclasses import dataclass
from itertools import chain

import pyspark.sql.functions as f
from pyspark.sql import Column, DataFrame, Window

from gentropy.common.processing import normalize_chromosome
from gentropy.common.session import Session
from gentropy.dataset.credible_set import CredibleSet
from gentropy.dataset.study_index import StudyIndex, StudyType
from gentropy.datasource.eqtlgen.finemapping import EqtlGenDataset, EqtlGenFinemapping


@dataclass
class EqtlGenStudyIndex:
    """Study index for the eQTLGen single-cell eQTLs and co-eQTLs.

    One study per cell type and feature with at least one credible set: a gene for the
    single-cell eQTLs, a pair of genes for the co-eQTLs. The gene of a co-eQTL study is `eGene`,
    the gene whose expression the eQTL changes. The sample size is the largest of the variants
    tested for the study: the co-eQTLs are a meta-analysis, so it changes between variants.
    """

    # (cell type, Cell Ontology term). CD4 T cells are CD4-positive, alpha-beta T cells for
    # both datasets, as for the FinnGen multiome and OneK1K studies.
    CELL_TYPES: tuple[tuple[str, str], ...] = (
        ("B", "CL_0000236"),
        ("CD4_T", "CL_0000624"),
        ("CD8_T", "CL_0000625"),
        ("DC", "CL_0000451"),
        ("Mono", "CL_0000576"),
        ("NK", "CL_0000623"),
    )

    @classmethod
    def _biosample(cls: type[EqtlGenStudyIndex]) -> Column:
        """Cell Ontology term of `cellType`.

        Returns:
            Column: The term, null for an unknown cell type.
        """
        return f.create_map(*[f.lit(x) for x in chain(*cls.CELL_TYPES)])[
            f.col("cellType")
        ]

    @staticmethod
    def map_genes(studies: DataFrame, target_index: DataFrame) -> DataFrame:
        """Resolve the gene symbol `eGene` to an Ensembl gene identifier.

        A symbol can name several genes. The gene is the one named by the symbol, approved or
        obsolete, on the chromosome of the feature, that overlaps the feature reported by
        eQTLGen (`featureStart`, `featureEnd`). Among several, an approved symbol is preferred,
        then the gene starting closest to the feature. A symbol with no such gene is left without
        a gene.

        Args:
            studies (DataFrame): One row per study with `eGene`, `featureChromosome`, `featureStart` and `featureEnd`.
            target_index (DataFrame): The target index, with `id`, `approvedSymbol`, `obsoleteSymbols` and `genomicLocation`.

        Returns:
            DataFrame: `studies` with `geneId`.

        Examples:
            >>> studies = spark.createDataFrame(
            ...     [("s1", "G", "1", 100, 200), ("s2", "H", "1", 100, 200)],
            ...     "studyId string, eGene string, featureChromosome string, featureStart int, featureEnd int",
            ... )
            >>> location = "struct<chromosome:string,start:bigint,end:bigint,strand:int>"
            >>> target = spark.createDataFrame(
            ...     [
            ...         ("E1", "G", [], ("2", 100, 200, 1)),
            ...         ("E2", "G", [], ("1", 150, 300, 1)),
            ...         ("E3", "X", [("G", "HGNC")], ("1", 120, 180, 1)),
            ...     ],
            ...     f"id string, approvedSymbol string, obsoleteSymbols array<struct<label:string,source:string>>, genomicLocation {location}",
            ... )
            >>> EqtlGenStudyIndex.map_genes(studies, target).orderBy("studyId").select("studyId", "geneId").show()
            +-------+------+
            |studyId|geneId|
            +-------+------+
            |     s1|    E2|
            |     s2|  NULL|
            +-------+------+
            <BLANKLINE>
        """
        location = f.col("genomicLocation")
        gene = [
            f.col("id").alias("geneId"),
            normalize_chromosome(location.getField("chromosome")).alias(
                "geneChromosome"
            ),
            location.getField("start").alias("geneStart"),
            location.getField("end").alias("geneEnd"),
        ]
        symbols = target_index.select(
            *gene, f.col("approvedSymbol").alias("symbol"), f.lit(True).alias("approved")
        ).unionByName(
            target_index.select(
                *gene,
                f.explode(f.col("obsoleteSymbols.label")).alias("symbol"),
                f.lit(False).alias("approved"),
            )
        )
        candidates = studies.join(
            symbols,
            (f.col("symbol") == f.col("eGene"))
            & (f.col("geneChromosome") == f.col("featureChromosome"))
            & (f.col("geneStart") <= f.col("featureEnd"))
            & (f.col("geneEnd") >= f.col("featureStart")),
            how="left",
        )
        best = Window.partitionBy("studyId").orderBy(
            f.desc("approved"),
            f.abs(f.col("geneStart") - f.col("featureStart")),
            "geneId",
        )
        return (
            candidates.withColumn("_rank", f.row_number().over(best))
            .filter(f.col("_rank") == 1)
            .drop("_rank", "symbol", "approved", "geneChromosome", "geneStart", "geneEnd")
        )

    @classmethod
    def from_source(
        cls: type[EqtlGenStudyIndex],
        session: Session,
        dataset: EqtlGenDataset,
        variants: DataFrame,
        credible_sets: CredibleSet,
        target_index_path: str | None = None,
    ) -> StudyIndex:
        """Build the study index of the studies with credible sets.

        Args:
            session (Session): Session object.
            dataset (EqtlGenDataset): The dataset of the variants.
            variants (DataFrame): Every variant tested, as given by `EqtlGenFinemapping.read`.
            credible_sets (CredibleSet): The credible sets of the variants.
            target_index_path (str | None): The target index, needed for the co-eQTLs only.

        Returns:
            StudyIndex: One study per cell type and feature with credible sets.

        Raises:
            ValueError: If the co-eQTLs are given no target index.
        """
        feature = (
            ["geneId"]
            if dataset is EqtlGenDataset.SINGLE_CELL_EQTL
            else ["eGene", "featureChromosome", "featureStart", "featureEnd"]
        )
        studies = (
            variants.groupBy("studyId")
            .agg(
                *[
                    f.first(c, ignorenulls=True).alias(c)
                    for c in ["cellType", "traitFromSource", *feature]
                ],
                f.max("sampleSize").alias("nSamples"),
            )
            .join(credible_sets.df.select("studyId").distinct(), on="studyId")
        )
        if dataset is EqtlGenDataset.COEQTL:
            if target_index_path is None:
                raise ValueError("The co-eQTLs need a target index to map eGene.")
            studies = cls.map_genes(
                studies, session.spark.read.parquet(target_index_path)
            )
            unmapped = studies.filter(f.col("geneId").isNull()).count()
            if unmapped:
                session.logger.warning(f"Studies without a gene for eGene: {unmapped}")
        unknown = {
            r.cellType
            for r in studies.select("cellType").distinct().collect()
            if r.cellType not in dict(cls.CELL_TYPES)
        }
        if unknown:
            session.logger.warning(f"Cell types without annotation: {sorted(unknown)}")
        return StudyIndex(
            _df=studies.select(
                "studyId",
                f.lit(EqtlGenFinemapping.PROJECT_ID).alias("projectId"),
                f.lit(StudyType.SCEQTL.value).alias("studyType"),
                "geneId",
                "traitFromSource",
                cls._biosample().alias("biosampleFromSourceId"),
                f.col("nSamples").cast("integer"),
                f.lit(False).alias("hasSumstats"),
            ),
            _schema=StudyIndex.get_schema(),
        )
