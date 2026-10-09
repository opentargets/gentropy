"""Common fixtures for Finngen tests."""

import json
from collections.abc import Callable
from unittest.mock import MagicMock

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import types as T

from gentropy.datasource.finngen.efo_mapping import EFOMapping


@pytest.fixture()
def finngen_phenotype_table_mock() -> str:
    """This is the data extracted from https://r11.finngen.fi/api/phenos."""
    data = json.dumps(
        [
            # NOTE: Study maps to single EFO trait.
            {
                "assoc_files": [
                    "/cromwell_root/pheweb/generated-by-pheweb/pheno_gz/AB1_ACTINOMYCOSIS.gz"
                ],
                "category": "I Certain infectious and parasitic diseases (AB1_)",
                "category_index": 1,
                "gc_lambda": {
                    "0.001": 0.93878,
                    "0.01": 0.96727,
                    "0.1": 0.85429,
                    "0.5": 0.52544,
                },
                "num_cases": 113,
                "num_cases_prev": 101,
                "num_controls": 399149,
                "num_controls_prev": 363227,
                "num_gw_significant": 0,
                "num_gw_significant_prev": 0,
                "phenocode": "AB1_ACTINOMYCOSIS",
                "phenostring": "Actinomycosis",
            },
            # NOTE: Study maps to multiple EFO traits.
            {
                "assoc_files": [
                    "/cromwell_root/pheweb/generated-by-pheweb/pheno_gz/GLUCOSE.gz"
                ],
                "category": "Glucose",
                "category_index": 28,
                "gc_lambda": {
                    "0.001": 1.1251,
                    "0.01": 1.062,
                    "0.1": 1.0531,
                    "0.5": 1.0599,
                },
                "num_cases": 43764,
                "num_cases_prev": 39231,
                "num_controls": 409969,
                "num_controls_prev": 372950,
                "num_gw_significant": 3,
                "num_gw_significant_prev": 3,
                "phenocode": "GLUCOSE",
                "phenostring": "Glucose",
            },
            # NOTE: Study does not map to EFO traits
            {
                "assoc_files": [
                    "/cromwell_root/pheweb/generated-by-pheweb/pheno_gz/SOME_OTHER_TRAIT.gz"
                ],
                "category": "SomeOtherTrait",
                "category_index": 28,
                "gc_lambda": {
                    "0.001": 1.1251,
                    "0.01": 1.062,
                    "0.1": 1.0531,
                    "0.5": 1.0599,
                },
                "num_cases": 43764,
                "num_cases_prev": 39231,
                "num_controls": 409969,
                "num_controls_prev": 372950,
                "num_gw_significant": 3,
                "num_gw_significant_prev": 3,
                "phenocode": "SOME_OTHER_TRAIT",
                "phenostring": "Some other trait",
            },
        ]
    )
    return data


@pytest.fixture()
def efo_mappings_mock() -> list[tuple[str, str, str]]:
    """EFO mappings mock based on https://raw.githubusercontent.com/opentargets/curation/24.09.1/mappings/disease/manual_string.tsv.

    Only required fields are extracted.
    """
    data = [
        (
            "STUDY",
            "PROPERTY_VALUE",
            "SEMANTIC_TAG",
        ),
        ("FinnGen r11", "Actinomycosis", "http://www.ebi.ac.uk/efo/EFO_0007128"),
        # NOTE: EFO does not map, as it's missing from the StudyIndex - hypothetical example.
        ("FinnGen r11", "Bleeding", "http://purl.obolibrary.org/obo/MP_0001914"),
        # NOTE: Two EFO traits for one disease should be collected to array - hypothetical example:
        # Glucose tolerance test & NMR Glucose
        ("FinnGen r11", "Glucose", "http://www.ebi.ac.uk/efo/EFO_0002571"),
        ("FinnGen r11", "Glucose", "http://www.ebi.ac.uk/efo/EFO_0004468"),
        # NOTE: EFO that does not map, due to study not from Finngen - hypothetical example.
        ("PheWAS 2024", "Glucose", "http://www.ebi.ac.uk/efo/EFO_0000001"),
    ]
    return data


@pytest.fixture()
def efo_mappings_df_mock(
    spark: SparkSession, efo_mappings_mock: list[tuple[str, str, str]]
) -> EFOMapping:
    """EFO mappings dataframe mock."""
    schema = T.StructType(
        [
            T.StructField("STUDY", T.StringType(), nullable=False),
            T.StructField("PROPERTY_VALUE", T.StringType(), nullable=False),
            T.StructField("SEMANTIC_TAG", T.StringType(), nullable=False),
        ]
    )
    data = spark.createDataFrame(data=efo_mappings_mock, schema=schema)
    return EFOMapping(df=data)


@pytest.fixture()
def urlopen_mock(
    efo_mappings_mock: list[tuple[str, str, str, str]],
    finngen_phenotype_table_mock: str,
) -> Callable[[str], MagicMock]:
    """Mock object for requesting urlopen objects with proper encoding.

    This mock object allows to call `read` and `readlines` methods on two endpoints:
    - https://finngen_phenotypes -> finngen_phenotype_table_mock
    - https://efo_mappings -> efo_mappings_mock

    The return values are mocks of the source data respectively.
    """

    def mock_response(url: str) -> MagicMock:
        """Mock urllib.request.urlopen."""
        match url:
            case "https://finngen_phenotypes":
                value = finngen_phenotype_table_mock
            case "https://efo_mappings":
                value = "\n".join(["\t".join(row) for row in efo_mappings_mock])
            case _:
                value = ""
        mock_open = MagicMock()
        mock_open.read.return_value = value.encode()
        mock_open.readlines.return_value = value.encode().splitlines(keepends=True)
        return mock_open

    return mock_response
