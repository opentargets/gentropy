"""Step to run FinnGen study index generation."""

from __future__ import annotations

from urllib.request import urlopen

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
        efo_curation_mapping_url: str = FinngenStudiesConfig().efo_curation_mapping_url,
    ) -> None:
        """Run FinnGen study index generation step.

        Args:
            session (Session): Session object.
            finngen_study_index_out (str): Output FinnGen study index path.
            finngen_phenotype_table_url (str): URL to the FinnGen phenotype table.
            finngen_release_prefix (str): FinnGen release prefix.
            finngen_summary_stats_url_prefix (str): FinnGen summary stats URL prefix.
            finngen_summary_stats_url_suffix (str): FinnGen summary stats URL suffix.
            efo_curation_mapping_url (str): URL to the EFO curation mapping file
        """
        study_index = FinnGenStudyIndex.from_source(
            session.spark,
            finngen_phenotype_table_url,
            finngen_release_prefix,
            finngen_summary_stats_url_prefix,
            finngen_summary_stats_url_suffix,
        )

        # NOTE: hack to allow spark to read directly from the URL.
        csv_data = urlopen(efo_curation_mapping_url).readlines()
        csv_rows = [row.decode("utf8") for row in csv_data]
        rdd = session.spark.sparkContext.parallelize(csv_rows)
        efo_curation_mapping = session.spark.read.csv(rdd, header=True, sep="\t")

        study_index_with_efo = FinnGenStudyIndex.join_efo_mapping(
            study_index,
            efo_curation_mapping,
            finngen_release_prefix,
        )
        study_index_with_efo.df.write.mode(session.write_mode).parquet(
            finngen_study_index_out
        )
