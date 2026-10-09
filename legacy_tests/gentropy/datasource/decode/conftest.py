"""Shared fixtures for deCODE datasource tests."""

from __future__ import annotations

from datetime import datetime

import pytest
from pyspark.sql import Row

from gentropy.common.session import Session
from gentropy.dataset.molecular_complex import MolecularComplex
from gentropy.datasource.decode.aptamer_metadata import AptamerMetadata
from gentropy.datasource.decode.manifest import deCODEManifest


@pytest.fixture()
def sample_manifest(session: Session) -> deCODEManifest:
    """A two-row deCODEManifest (one SMP, one RAW) for reuse across decode tests."""
    rows = [
        Row(
            projectId="deCODE-proteomics-smp",
            studyId="deCODE-proteomics-smp_Proteomics_SMP_PC0_10000_2_GENE1_PROTEIN1_00000001",
            hasSumstats=True,
            summarystatsLocation="s3a://my_bucket/some_folder/Proteomics_SMP_PC0_10000_2_GENE1_PROTEIN1_00000001.txt.gz",
            size="927.2 MiB",
            accessionTimestamp=datetime(2022, 5, 29, 9, 27, 28),
        ),
        Row(
            projectId="deCODE-proteomics-raw",
            studyId="deCODE-proteomics-raw_Proteomics_PC0_10001_1_GENE2__SOME_PROTEIN_2_00000001",
            hasSumstats=True,
            summarystatsLocation="s3a://my_bucket/some_folder/Proteomics_PC0_10001_1_GENE2__SOME_PROTEIN_2_00000001.txt.gz",
            size="926.0 MiB",
            accessionTimestamp=datetime(2022, 5, 29, 9, 27, 35),
        ),
    ]
    df = session.spark.createDataFrame(rows, schema=deCODEManifest.get_schema())
    return deCODEManifest(_df=df)


@pytest.fixture()
def sample_aptamer_metadata(session: Session) -> AptamerMetadata:
    """A small AptamerMetadata dataset covering single-target and complex cases."""
    rows = [
        Row(
            aptamerId="10000-2",
            targetName="GENE1",
            targetFullName="Gene 1 Full Name",
            isProteinComplex=False,
            targetMetadata=[Row(geneSymbol="GENE1", proteinId="P12345")],
        ),
        Row(
            aptamerId="10001-1",
            targetName="GENE2",
            targetFullName="Gene 2 Full Name",
            isProteinComplex=False,
            targetMetadata=[Row(geneSymbol="GENE2", proteinId="Q99999")],
        ),
        Row(
            aptamerId="20000-1",
            targetName="COMPLEX1",
            targetFullName="Complex Protein 1",
            isProteinComplex=True,
            targetMetadata=[
                Row(geneSymbol="GENEA", proteinId="P11111"),
                Row(geneSymbol="GENEB", proteinId="P22222"),
            ],
        ),
    ]
    df = session.spark.createDataFrame(rows, schema=AptamerMetadata.get_schema())
    return AptamerMetadata(_df=df)


@pytest.fixture()
def empty_molecular_complex(session: Session) -> MolecularComplex:
    """An empty MolecularComplex for use when no complex annotation is needed."""
    df = session.spark.createDataFrame([], schema=MolecularComplex.get_schema())
    return MolecularComplex(_df=df)
