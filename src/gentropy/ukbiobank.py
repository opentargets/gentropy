"""Step to run UKBiobank study table ingestion."""

from __future__ import annotations

from gentropy.common.session import Session
from gentropy.datasource.ukbiobank.study_index import UKBiobankStudyIndex


class UKBiobankStep:
    """UKBiobank study table ingestion step."""

    def __init__(
        self, session: Session, ukbiobank_manifest: str, ukbiobank_study_index_out: str
    ) -> None:
        """Run UKBB ingestion step.

        Args:
            session (Session): Session object.
            ukbiobank_manifest (str): Input UKBiobank manifest path.
            ukbiobank_study_index_out (str): Output UKBiobank study index path.
        """
        # Read in the UKBiobank manifest tsv file.
        df = session.spark.read.csv(
            ukbiobank_manifest, sep="\t", header=True, inferSchema=True
        )

        # Parse the study index data.
        ukbiobank_study_index = UKBiobankStudyIndex.from_source(df)

        # Write the output.
        ukbiobank_study_index.df.write.mode(session.write_mode).parquet(
            ukbiobank_study_index_out
        )
