"""Step to run FinnGen study index generation."""

from __future__ import annotations

from pyspark import SparkFiles

from gentropy.common.session import Session
from gentropy.config import FinngenStudiesConfig
from gentropy.datasource.finngen.study_index import FinnGenStudyIndex


class FinnGenStudiesStep:
    """FinnGen study index generation step."""

    def __init__(
        self,
        session: Session,
        finngen_study_index_out: str,
        finngen_phenotype_table_url: str = FinngenStudiesConfig().finngen_phenotype_table_url,
        finngen_release_prefix: str = FinngenStudiesConfig().finngen_release_prefix,
        finngen_summary_stats_url_prefix: str = FinngenStudiesConfig().finngen_summary_stats_url_prefix,
        finngen_summary_stats_url_suffix: str = FinngenStudiesConfig().finngen_summary_stats_url_suffix,
        efo_curation_mapping_path: str = FinngenStudiesConfig().efo_curation_mapping_path,
    ) -> None:
        """Run FinnGen study index generation step.

        Args:
            session (Session): Session object.
            finngen_study_index_out (str): Output FinnGen study index path.
            finngen_phenotype_table_url (str): URL to the FinnGen phenotype table.
            finngen_release_prefix (str): FinnGen release prefix.
            finngen_summary_stats_url_prefix (str): FinnGen summary stats URL prefix.
            finngen_summary_stats_url_suffix (str): FinnGen summary stats URL suffix.
            efo_curation_mapping_path (str): Path to the EFO curation mapping file
        """
        study_index = FinnGenStudyIndex.from_source(
            session.spark,
            finngen_phenotype_table_url,
            finngen_release_prefix,
            finngen_summary_stats_url_prefix,
            finngen_summary_stats_url_suffix,
        )

        session.spark.sparkContext.addFile(efo_curation_mapping_path)
        efo_stem = efo_curation_mapping_path.split("/")[-1]
        efo_hadoop_file = "file:///" + SparkFiles.get(efo_stem)
        efo_curation_mapping = session.spark.read.csv(
            efo_hadoop_file, header=True, sep="\t"
        )
        study_index_with_efo = FinnGenStudyIndex.join_efo_mapping(
            study_index,
            finngen_release_prefix,
            efo_curation_mapping,
        )
        study_index_with_efo.df.write.mode(session.write_mode).parquet(
            finngen_study_index_out
        )
