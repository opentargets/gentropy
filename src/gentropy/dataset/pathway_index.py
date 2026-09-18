"""Pathway index dataset."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from pyspark.sql import functions as f
from pyspark.sql.types import ArrayType, StringType

from gentropy.common.schemas import parse_spark_schema
from gentropy.dataset.dataset import Dataset

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.types import StructType

    from gentropy.common.session import Session
    from gentropy.dataset.target_index import TargetIndex


@dataclass
class PathwayIndex(Dataset):
    """Gene set membership of a pathway library.

    One row per pathway, holding the gene symbols that make up the gene set, the identifier of
    the pathway in its source ontology, and the Ensembl gene identifiers the symbols resolve
    to. This is the reference that disease-pathway enrichment results are computed against,
    and it is what turns an enriched pathway back into a set of genes.

    The library used so far merges three sources, and one row of each looks like this:

    | pathway                                                    | pathwayId     | source           | geneSymbols                        |
    | ---------------------------------------------------------- | ------------- | ---------------- | ---------------------------------- |
    | `'De Novo' AMP Biosynthetic Process (GO:0044208) [GO BP]`   | `GO:0044208`  | `GO BP`          | `[ADSL, ADSS1, ADSS2, ATIC, ...]`  |
    | `2-LTR circle formation [Reactome]`                        | `R-HSA-164843`| `Reactome`       | `[BANF1, HMGA1, LIG4, PSIP1, ...]` |
    | `HALLMARK_ADIPOGENESIS [MSigDB Hallmark]`                  | null          | `MSigDB Hallmark`| `[ABCA1, ABCB8, ACAA2, ACADL, ...]`|

    `pathway` is kept verbatim, including the bracketed source tag, because it is the only key
    that links the library to the enrichment results, which store the same string.
    """

    SOURCE_PATTERN = r"\[([^\]]+)\]\s*$"
    """Pattern extracting the library a pathway came from out of its name, e.g. `[Reactome]`."""

    @classmethod
    def get_schema(cls: type[PathwayIndex]) -> StructType:
        """Provide the schema for the PathwayIndex dataset.

        Returns:
            StructType: The schema of the PathwayIndex dataset.
        """
        return parse_spark_schema("pathway_index.json")

    @classmethod
    def from_gmt(cls: type[PathwayIndex], session: Session, path: str) -> PathwayIndex:
        """Parse a gene matrix transposed (GMT) file into a PathwayIndex.

        A GMT file holds one tab separated gene set per line: the pathway name, an identifier
        or description, and then the gene symbols. A line of the library used so far reads

        ```
        2-LTR circle formation [Reactome]	R-HSA-164843	BANF1	HMGA1	LIG4	PSIP1
        ```

        and comes back as a single row with `pathway` = `2-LTR circle formation [Reactome]`,
        `pathwayId` = `R-HSA-164843`, `source` = `Reactome` and `geneSymbols` =
        `[BANF1, HMGA1, LIG4, PSIP1]`. `geneIds` is left null; call
        [`resolve_gene_ids`][gentropy.dataset.pathway_index.PathwayIndex.resolve_gene_ids] to
        fill it in against a release of the target index.

        The second field is an identifier in every source that has one - `GO:0044208` for Gene
        Ontology, `R-HSA-164843` for Reactome - and empty for MSigDB Hallmark, so `pathwayId`
        is nullable.

        Args:
            session (Session): Spark session
            path (str): Path to the GMT file

        Returns:
            PathwayIndex: Pathway index dataset with no gene identifiers resolved yet
        """
        columns = f.split(f.col("value"), "\t")
        pathway = f.trim(columns.getItem(0))
        return cls(
            _df=(
                session.spark.read.text(path)
                .filter(f.trim(f.col("value")) != "")
                .select(
                    pathway.alias("pathway"),
                    f.nullif(f.trim(columns.getItem(1)), f.lit("")).alias("pathwayId"),
                    f.nullif(
                        f.regexp_extract(pathway, cls.SOURCE_PATTERN, 1), f.lit("")
                    ).alias("source"),
                    f.array_distinct(
                        f.filter(
                            f.transform(
                                f.slice(columns, 3, f.size(columns)),
                                lambda gene: f.trim(gene),
                            ),
                            lambda gene: gene != "",
                        )
                    ).alias("geneSymbols"),
                    f.lit(None).cast(ArrayType(StringType(), False)).alias("geneIds"),
                )
                .filter(f.size("geneSymbols") > 0)
            ),
            _schema=cls.get_schema(),
        )

    def resolve_gene_ids(
        self: PathwayIndex, target_index: TargetIndex
    ) -> PathwayIndex:
        """Resolve the gene symbols of every gene set into Ensembl gene identifiers.

        Symbols are looked up in
        [`TargetIndex.symbols_lut`][gentropy.dataset.target_index.TargetIndex.symbols_lut],
        which covers approved and obsolete symbols. A symbol that resolves to more than one
        gene contributes all of them, and one that resolves to none is dropped, so the mapping
        is release specific: against Open Targets 26.03, 16,733 of the 17,246 symbols of the
        library used so far resolve, the remainder being tRNA and immunoglobulin segment names.

        Args:
            target_index (TargetIndex): Target index of the release the identifiers should
                belong to

        Returns:
            PathwayIndex: Pathway index dataset with `geneIds` filled in
        """
        gene_ids = (
            self.df.select("pathway", f.explode("geneSymbols").alias("geneSymbol"))
            .join(
                target_index.symbols_lut().select("geneSymbol", "geneId").distinct(),
                "geneSymbol",
                "inner",
            )
            .groupBy("pathway")
            .agg(f.array_sort(f.collect_set("geneId")).alias("resolvedGeneIds"))
        )
        return PathwayIndex(
            _df=(
                self.df.drop("geneIds")
                .join(gene_ids, "pathway", "left")
                .withColumnRenamed("resolvedGeneIds", "geneIds")
                .select(*[field.name for field in self.get_schema().fields])
            ),
            _schema=self.get_schema(),
        )

    def gene_membership(self: PathwayIndex) -> DataFrame:
        """Explode the gene sets into one row per pathway and gene identifier.

        Raises:
            ValueError: If the gene identifiers have not been resolved yet.

        Returns:
            DataFrame: Dataframe with `pathway` and `geneId` columns.
        """
        if self.df.filter(f.col("geneIds").isNotNull()).limit(1).count() == 0:
            raise ValueError(
                "The pathway index carries no gene identifiers. Build it with "
                "`PathwayIndex.resolve_gene_ids` before asking for gene membership."
            )
        return self.df.select(
            "pathway", f.explode("geneIds").alias("geneId")
        ).distinct()
