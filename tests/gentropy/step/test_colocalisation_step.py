"""Test colocalisation step."""

from pathlib import Path
from typing import Any, Literal

import pytest

from gentropy.colocalisation import ColocalisationStep
from gentropy.common.session import Session
from gentropy.dataset.colocalisation import Colocalisation
from gentropy.dataset.credible_set import CredibleSet, CredibleSetVariant
from gentropy.dataset.study_locus import StudyLocusQualityCheck
from gentropy.method.colocalisation import (
    ColocalisationMethod,
    InvalidColocalisationMethodError,
)
from gentropy.method.colocalisation.coloc import Coloc
from gentropy.method.colocalisation.ecaviar import ECaviar
from gentropy.method.colocalisation.model import ColocalisationMethodInterface


@pytest.mark.step_test
class TestColocalisationStep:
    """Test colocalisation steps."""

    @pytest.fixture(autouse=True)
    def _setup(self, session: Session, tmp_path: Path) -> None:
        """Write credible sets and their variants for testing.

        Four single-variant GWAS credible sets: two pairs that share their lead.
        """
        # (id, studyId, lead, locusStart, locusEnd, pValueMantissa, pValueExponent,
        #  logBayesFactor, posteriorProbability, reportedBeta)
        rows = [
            (
                "-1299941111165481046",
                "GCST90269661",
                "1_62634374_G_GA",
                62116600,
                63176657,
                8.294741,
                -72,
                303.2017476882394,
                1.0,
                -0.07779708137213309,
            ),
            (
                "-1245591334543437941",
                "GCST90024601",
                "1_62725906_C_A",
                62275115,
                62861709,
                1.0845997,
                -12,
                2087.4457573345685,
                0.9999999999381545,
                0.20241232721094407,
            ),
            (
                "-0.20241232721094407",
                "GCST90025461",
                "1_62725906_C_A",
                62335572,
                62883302,
                5.0753098,
                -10,
                2107.38950418228,
                0.9999999999454303,
                0.20330391077149534,
            ),
            (
                "-2271857845883525223",
                "GCST90269580",
                "1_62634374_G_GA",
                62192511,
                63034021,
                1.0077391,
                -54,
                249.20354469210795,
                1.0,
                -0.07071263272378725,
            ),
        ]
        out_of_sample_ld = [StudyLocusQualityCheck.OUT_OF_SAMPLE_LD.value]
        credible_sets = [
            (
                id,
                "1",
                start,
                end,
                study,
                "gwas",
                lead,
                "SuSiE-inf",
                None,
                None,
                1.0,
                1.0,
                out_of_sample_ld,
            )
            for id, study, lead, start, end, *_ in rows
        ]
        variants: list[tuple[Any, ...]] = [
            (
                id,
                lead,
                mantissa,
                exponent,
                None,
                None,
                beta,
                None,
                posterior,
                log_bf,
                None,
                None,
                None,
                None,
                None,
                None,
                [],
            )
            for id, _, lead, _, _, mantissa, exponent, log_bf, posterior, beta in rows
        ]
        self.credible_set_path = str(tmp_path / "credible_set")
        self.credible_set_variant_path = str(tmp_path / "credible_set_variant")
        session.spark.createDataFrame(
            credible_sets, schema=CredibleSet.get_schema()
        ).write.parquet(self.credible_set_path)
        session.spark.createDataFrame(
            variants, schema=CredibleSetVariant.get_schema()
        ).write.parquet(self.credible_set_variant_path)
        self.coloc_path = str(tmp_path / "colocalisation")

    @pytest.mark.parametrize(
        ["label", "expected_method"],
        [
            pytest.param("coloc", Coloc, id="coloc method"),
            pytest.param("ecaviar", ECaviar, id="ecaviar method"),
            pytest.param("ECaviar", ECaviar, id="uppercase label"),
        ],
    )
    def test_colocalisation_method_enum(
        self, label: str, expected_method: type[ColocalisationMethodInterface]
    ) -> None:
        """Test colocalisation method enum."""
        method = ColocalisationMethod.get_method_class(label)
        assert method is expected_method, (
            "Incorrect colocalisation class returned by ColocalisationMethod.get_method_class(label)"
        )

    def test_label_with_invalid_method(self) -> None:
        """Test what happens when invalid method_label is passed to the _get_colocalisation_class."""
        with pytest.raises(InvalidColocalisationMethodError):
            ColocalisationMethod.get_method_class("NewMethod")

    @pytest.mark.parametrize(
        ["coloc_method", "expected_data"],
        [
            pytest.param(
                "ecaviar",
                {
                    "clpp": [1.0, 1.0],
                    "colocalisationMethod": ["eCAVIAR", "eCAVIAR"],
                    "leftLocusId": [
                        "-1245591334543437941",
                        "-2271857845883525223",
                    ],
                    "rightLocusId": [
                        "-0.20241232721094407",
                        "-1299941111165481046",
                    ],
                },
                id="ecaviar",
            ),
            pytest.param(
                "coloc",
                {
                    "h4": [1.0, 1.0],
                    "h3": [0.0, 0.0],
                    "h2": [0.0, 0.0],
                    "h1": [0.0, 0.0],
                    "h0": [0.0, 0.0],
                    "colocalisationMethod": ["COLOC", "COLOC"],
                    "leftLocusId": [
                        "-1245591334543437941",
                        "-2271857845883525223",
                    ],
                    "rightLocusId": [
                        "-0.20241232721094407",
                        "-1299941111165481046",
                    ],
                },
                id="coloc",
            ),
        ],
    )
    def test_colocalise(
        self,
        coloc_method: Literal["coloc", "ecaviar"],
        expected_data: dict[str, list[float] | list[str]],
        session: Session,
    ) -> None:
        """Test colocalise method."""
        ColocalisationStep(
            session=session,
            credible_set_path=self.credible_set_path,
            credible_set_variant_path=self.credible_set_variant_path,
            coloc_path=self.coloc_path,
            colocalisation_method=coloc_method,
        )

        coloc_dataset = Colocalisation.from_parquet(
            session, self.coloc_path, recursiveFileLookup=True
        )

        assert not (Path(self.coloc_path) / coloc_method.lower()).exists()
        for column, expected_values in expected_data.items():
            values = [c[column] for c in coloc_dataset.df.collect()]
            for v, e in zip(values, expected_values):
                if isinstance(e, float):
                    assert e == pytest.approx(v, 1e-1), (
                        f"Incorrect value {v} at {column} found in {coloc_method}, expected {e}"
                    )
                else:
                    assert e == v, (
                        f"Incorrect value {v} at {column} found in {coloc_method}, expected {e}"
                    )
