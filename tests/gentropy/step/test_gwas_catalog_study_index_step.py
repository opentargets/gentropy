"""Test GWAS Catalog Study Index step."""

import pytest
from pyspark.sql import DataFrame
from pyspark.sql import types as t

from gentropy import Session


@pytest.fixture
def download_studies(session: Session) -> DataFrame:
    """Fixture returning an example of GWAS Catalog studies files.

    The original files can be found in the ftp.ebi.ac.uk/pub/databases/gwas/releases/latest/.
    """
    data = [
        # European ancestry only
        (
            "STUDY_A",
            "1",
            "AUTHOR_A",
            "2007-01-16",
            "JOURNAL_A",
            "TITLE_A",
            "TRAIT_A",
            "500 European ancestry cases, 500 European ancestry controls",
            "http://www.ebi.ac.uk/efo/TRAIT_A",
            None,
            "COHORT_A",
        ),
        # Finnish - broad ancestry = European
        (
            "STUDY_B",
            "2",
            "AUTHOR_B",
            "2022-03-28",
            "JOURNAL_B",
            "TITLE_B",
            "TRAIT_B",
            "10,123 Finnish ancestry individuals",
            "http://www.ebi.ac.uk/efo/TRAIT_B",
            None,
            "COHORT_B",
        ),
        # Finnish - broad ancestry
        (
            "STUDY_B1",
            "2",
            "AUTHOR_B",
            "2022-03-28",
            "JOURNAL_B",
            "TITLE_B",
            "TRAIT_B",
            "10,123 Finnish European ancestry individuals",
            "http://www.ebi.ac.uk/efo/TRAIT_B",
            None,
            "COHORT_B",
        ),
        # Icelandic - broad ancestry = European
        (
            "STUDY_C",
            "3",
            "AUTHOR_C",
            "2022-01-25",
            "JOURNAL_C",
            "TITLE_C",
            "TRAIT_C",
            "5,364 Icelandic ancestry individuals",
            "http://purl.obolibrary.org/obo/TRAIT_C",
            None,
            "COHORT_C",
        ),
        # European with Non-Finnish European ancestry samples
        (
            "STUDY_D",
            "4",
            "AUTHOR_D",
            "2024-03-06",
            "JOURNAL_D",
            "TITLE_D",
            "TRAIT_D",
            "100,628 Non-Finnish European ancestry individuals",
            "http://purl.obolibrary.org/obo/TRAIT_D, http://www.ebi.ac.uk/efo/TRAIT_E",
            None,
            "COHORT_D",
        ),
        (
            "STUDY_D",
            "4",
            "AUTHOR_D",
            "2024-03-06",
            "JOURNAL_D",
            "TITLE_D",
            "TRAIT_D",
            "100,628 Non-Finnish European ancestry individuals with finnish samples",
            "http://purl.obolibrary.org/obo/TRAIT_D, http://www.ebi.ac.uk/efo/TRAIT_E",
            None,
            "COHORT_D",
        ),
    ]
    schema = t.StructType(
        [
            t.StructField("STUDY ACCESSION", t.StringType(), True),
            t.StructField("PUBMED ID", t.StringType(), True),
            t.StructField("FIRST AUTHOR", t.StringType(), True),
            t.StructField("DATE", t.StringType(), True),
            t.StructField("JOURNAL", t.StringType(), True),
            t.StructField("STUDY", t.StringType(), True),
            t.StructField("DISEASE/TRAIT", t.StringType(), True),
            t.StructField("INITIAL SAMPLE SIZE", t.StringType(), True),
            t.StructField("MAPPED_TRAIT_URI", t.StringType(), True),
            t.StructField("MAPPED BACKGROUND TRAIT URI", t.StringType(), True),
            t.StructField("COHORT", t.StringType(), True),
        ]
    )
    return session.spark.createDataFrame(data, schema)


@pytest.fixture
def download_ancestries(session: Session) -> DataFrame:
    """Fixture returning an example of GWAS Catalog ancestries table.

    The original files can be found in the ftp.ebi.ac.uk/pub/databases/gwas/releases/latest/.
    """
    data = [
        (
            "STUDY_A",
            "initial",
            "500",
            "European",
            "500 European ancestry cases, 500 European ancestry controls",
        ),
        (
            "STUDY_A",
            "replication",
            "500",
            "European",
            "500 European ancestry cases, 500 European ancestry controls",
        ),
        (
            "STUDY_B",
            "initial",
            "10123",
            "European",
            "10,123 Finnish ancestry individuals",
        ),
        (
            "STUDY_B1",
            "initial",
            "10123",
            "European",
            "10,123 Finnish European ancestry individuals",
        ),
        (
            "STUDY_C",
            "initial",
            "5364",
            "European",
            "5,364 Icelandic ancestry individuals",
        ),
        (
            "STUDY_D",
            "initial",
            "100628",
            "European",
            "100,628 Non-Finnish European ancestry individuals",
        ),
    ]
    schema = t.StructType(
        [
            t.StructField("STUDY ACCESSION", t.StringType(), True),
            t.StructField("STAGE", t.StringType(), True),
            t.StructField("NUMBER OF INDIVIDUALS", t.StringType(), True),
            t.StructField("BROAD ANCESTRAL CATEGORY", t.StringType(), True),
            t.StructField("INITIAL SAMPLE DESCRIPTION", t.StringType(), True),
        ]
    )
    return session.spark.createDataFrame(data, schema)


class TestStudyIndexGwasCatalogParser:
    """Test GWAS Catalog Study Index step."""

    def test_parser_with_data_subset(
        self, download_ancestries: DataFrame, download_studies: DataFrame
    ) -> None:
        """Test GWAS Catalog Study Index step."""
        from gentropy.datasource.gwas_catalog.study_index import (
            StudyIndexGWASCatalogParser,
        )

        res = StudyIndexGWASCatalogParser.from_source(
            download_studies, download_ancestries
        ).df
        unique_studies = res.select("studyId").distinct().count()
        assert unique_studies == 5, "Should have 5 unique studyIndex values."

        ld_structures = (
            res.select("studyId", "ldPopulationStructure").distinct().collect()
        )
        assert all(len(row["ldPopulationStructure"]) == 1 for row in ld_structures)
        ld_structures = [
            (row["studyId"], row["ldPopulationStructure"][0]["ldPopulation"])
            for row in ld_structures
        ]
        expected_ld_structures = [
            ("STUDY_A", "nfe"),
            ("STUDY_B", "fin"),
            ("STUDY_B1", "fin"),
            ("STUDY_C", "nfe"),
            ("STUDY_D", "nfe"),
        ]
        assert ld_structures == expected_ld_structures, "LD do not match"
