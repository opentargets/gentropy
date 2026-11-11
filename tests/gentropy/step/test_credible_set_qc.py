"""Test credible set qc step."""

from pathlib import Path

import pytest
from pyspark.sql import functions as f

from gentropy.common.session import Session
from gentropy.credible_set_qc import CredibleSetQCStep
from gentropy.dataset.credible_set import CredibleSet, CredibleSetVariant


@pytest.mark.step_test
class TestCredibleSetQCStep:
    """Test credible set qc."""

    @pytest.fixture(autouse=True)
    def _setup(self, session: Session, tmp_path: Path) -> None:
        """Write credible sets and their variants for testing, one directory per credible set.

        Six credible sets: A three times, as if fine-mapped thrice; B with a lead p-value above
        the threshold; C with a purity below it; D passing. A with the highest
        logBayesFactor and D are expected to remain.
        """
        self.purity_min_r2 = 0.01
        self.p_value_threshold = 1e-5
        self.n_partitions = 1
        # (id, lead, logBayesFactor, purityMinR2, lead p-value exponent)
        credible_sets = [
            ("A", "1_100_G_GA", 1.0, 1.0, -6),
            ("A", "1_100_G_GA", 3.0, 1.0, -6),
            ("A", "1_100_G_GA", 2.0, 1.0, -6),
            ("B", "1_200_G_GA", 1.0, 1.0, -4),
            ("C", "1_300_G_GA", 1.0, 0.001, -6),
            ("D", "1_400_G_GA", 1.0, 1.0, -6),
        ]
        cs_df = session.spark.createDataFrame(
            [
                (
                    id,
                    "1",
                    1,
                    1000,
                    "GCST1",
                    "gwas",
                    lead,
                    "SuSiE-inf",
                    log10bf,
                    None,
                    None,
                    purity,
                    [],
                )
                for id, lead, log10bf, purity, _ in credible_sets
            ],
            CredibleSet.get_schema(),
        )
        variant_df = session.spark.createDataFrame(
            [
                (
                    id,
                    lead,
                    1.0,
                    exponent,
                    None,
                    None,
                    0.1,
                    0.01,
                    0.9,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    [],
                )
                for id, lead, _, _, exponent in credible_sets
            ],
            CredibleSetVariant.get_schema(),
        )

        # Mimic fine-mapping output, written per credible set and read recursively.
        self.cs_path = str(tmp_path / "credible_set")
        self.variant_path = str(tmp_path / "credible_set_variant")
        for run, row in enumerate(cs_df.collect()):
            cs_df.filter(
                (f.col("id") == row.id)
                & (f.col("logBayesFactor") == row.logBayesFactor)
            ).write.parquet(f"{self.cs_path}/{row.id}/{run}")
            variant_df.filter(f.col("credibleSetId") == row.id).limit(1).write.parquet(
                f"{self.variant_path}/{row.id}/{run}"
            )
        self.output_path = str(tmp_path / "clean_credible_set")
        self.variant_output_path = str(tmp_path / "clean_credible_set_variant")

    def test_step(self, session: Session) -> None:
        """Invoke the step to check if it works correctly."""
        CredibleSetQCStep(
            session=session,
            credible_set_path=self.cs_path,
            credible_set_variant_path=self.variant_path,
            credible_set_output_path=self.output_path,
            credible_set_variant_output_path=self.variant_output_path,
            p_value_threshold=self.p_value_threshold,
            purity_min_r2=self.purity_min_r2,
            ld_index_path=None,
            study_index_path=None,
            ld_min_r2=0.8,
            n_partitions=self.n_partitions,
        )

        partitions = [
            p for p in Path(self.output_path).iterdir() if str(p).endswith(".parquet")
        ]
        assert len(partitions) == self.n_partitions, (
            "Incorrect number of partitions in the output."
        )
        cs = CredibleSet.from_parquet(session, self.output_path)
        data = {row.id: row.logBayesFactor for row in cs.df.collect()}
        # A with the highest logBayesFactor, and D.
        assert data == {"A": 3.0, "D": 1.0}
        variants = CredibleSetVariant.from_parquet(session, self.variant_output_path)
        assert sorted(
            (row.credibleSetId, row.variantId) for row in variants.df.collect()
        ) == [("A", "1_100_G_GA"), ("D", "1_400_G_GA")]
