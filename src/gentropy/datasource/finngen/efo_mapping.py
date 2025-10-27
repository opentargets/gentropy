"""EFO-finnGen phenotype mapping build based on manual curation."""

from __future__ import annotations

from urllib.request import urlopen

from pyspark.sql import DataFrame
from pyspark.sql import functions as f
from pyspark.sql import types as t

from gentropy import Session, StudyIndex


class EFOMapping:
    """EFO-finnGen phecode mapping datasource."""

    required_columns = {"STUDY", "PROPERTY_VALUE", "SEMANTIC_TAG"}

    def __init__(self, df: DataFrame) -> None:
        """Initialize the EFO curation.

        Args:
            df (DataFrame): DataFrame containing the EFO curation data.
        """
        self.df = df

    @classmethod
    def from_path(cls, session: Session, efo_curation_path: str) -> EFOMapping:
        """Load the EFO curation from a specified path.

        Note:
            This method asserts that the EFO curation file is tab-delimited and contains header with following columns:
            ```
            |-- STUDY: string (nullable = true)           # required
            |-- PROPERTY_VALUE: string (nullable = true)  # required
            |-- SEMANTIC_TAG: string (nullable = true)    # required
            ```

        Note:
            Example of the file can be found in https://raw.githubusercontent.com/opentargets/curation/refs/heads/master/mappings/disease/manual_string.tsv.

        Args:
            session (Session): Session object.
            efo_curation_path (str): Path to the EFO curation file.

        Returns:
            EFOMapping: Loaded EFO curation object.

        Raises:
            AssertionError: If the EFO curation file does not contain the required columns.

        """
        if efo_curation_path.startswith("http"):
            csv_data = urlopen(efo_curation_path).readlines()
            csv_rows: list[str] = [row.decode("utf8") for row in csv_data]
            rdd = session.spark.sparkContext.parallelize(csv_rows)
            # NOTE: type annotations for spark.read.csv miss the fact that the first param can be [RDD[str]]
            efo_curation_mapping = session.spark.read.csv(rdd, header=True, sep="\t")
        else:
            efo_curation_mapping = session.spark.read.csv(
                efo_curation_path,
                sep="\t",
                header=True,
            )
        assert cls.required_columns.issubset(
            set(efo_curation_mapping.columns)
        ), f"EFO curation file must contain the following columns: {cls.required_columns}."
        columns = [
            f.col(col).cast(t.StringType()).alias(col) for col in cls.required_columns
        ]
        df = efo_curation_mapping.select(*columns)
        return cls(df=df)

    def annotate_study_index(
        self,
        study_index: StudyIndex,
        finngen_release: str = "R12",
    ) -> StudyIndex:
        """Add EFO mapping to the Finngen study index table.

        This function performs inner join on table of EFO mappings to the study index table by trait name.
        All studies without EFO traits are dropped. The EFO mappings are then aggregated into lists per
        studyId.

        NOTE: preserve all studyId entries even if they don't have EFO mappings.
        This is to avoid discrepancies between `study_index` and `credible_set` `studyId` column.
        The rows with missing EFO mappings will be dropped in the study_index validation step.

        Args:
            study_index (StudyIndex): Study index table.
            finngen_release (str): FinnGen release.

        Returns:
            StudyIndex: Study index table with added EFO mappings.
        """
        efo_mappings = (
            self.df.withColumn("STUDY", f.upper(f.col("STUDY")))
            .filter(f.col("STUDY").contains("FINNGEN"))
            .filter(f.upper(f.col("STUDY")).contains(finngen_release))
            .select(
                f.regexp_replace(f.col("SEMANTIC_TAG"), r"^.*/", "").alias(
                    "traitFromSourceMappedId"
                ),
                f.col("PROPERTY_VALUE").alias("traitFromSource"),
            )
        )

        si_df = study_index.df.join(
            efo_mappings, on="traitFromSource", how="left_outer"
        )
        common_cols = [c for c in si_df.columns if c != "traitFromSourceMappedId"]
        si_df = si_df.groupby(common_cols).agg(
            f.collect_list("traitFromSourceMappedId").alias("traitFromSourceMappedIds")
        )
        return StudyIndex(_df=si_df)
