"""Test L2G step."""

from pathlib import Path

from gentropy import Session
from gentropy.l2g import LocusToGeneStep


class TestLocusToGeneStep:
    """Test Locus to Gene step."""

    def test_step(self, tmp_path: Path, session: Session) -> None:
        """Test Locus to Gene step execution."""
        input_path = Path("tests/gentropy/data_samples/l2g_tests")
        study_loci = (input_path / "credible_set_slice.parquet").as_posix()
        gold_standard = (input_path / "l2g_gold_standard_slice.json").as_posix()
        l2g_matrix = (input_path / "l2g_feature_matrix_slice.parquet").as_posix()

        hyperparameters = {
            "n_estimators": 10,
            "max_depth": 5,
            "random_state": 42,
        }

        LocusToGeneStep(
            session,
            run_mode="train",
            hyperparameters=hyperparameters,
            download_from_hub=True,
            cross_validate=True,
            credible_set_path=study_loci,
            gold_standard_curation_path=gold_standard,
            feature_matrix_path=l2g_matrix,
            features_list=[
                # max CLPP for each (study, locus, gene) aggregating over a specific qtl type
                "eQtlColocClppMaximum",
                "pQtlColocClppMaximum",
                "sQtlColocClppMaximum",
                # max H4 for each (study, locus, gene) aggregating over a specific qtl type
                "eQtlColocH4Maximum",
                "pQtlColocH4Maximum",
                "sQtlColocH4Maximum",
                # max CLPP for each (study, locus, gene) aggregating over a specific qtl type and in relation with the mean in the vicinity
                "eQtlColocClppMaximumNeighbourhood",
                "pQtlColocClppMaximumNeighbourhood",
                "sQtlColocClppMaximumNeighbourhood",
                # max H4 for each (study, locus, gene) aggregating over a specific qtl type and in relation with the mean in the vicinity
                "eQtlColocH4MaximumNeighbourhood",
                "pQtlColocH4MaximumNeighbourhood",
                "sQtlColocH4MaximumNeighbourhood",
                # distance to gene footprint
                "distanceSentinelFootprint",
                "distanceSentinelFootprintNeighbourhood",
                "distanceFootprintMean",
                "distanceFootprintMeanNeighbourhood",
                # distance to gene tss
                "distanceTssMean",
                "distanceTssMeanNeighbourhood",
                "distanceSentinelTss",
                "distanceSentinelTssNeighbourhood",
                # vep
                "vepMaximum",
                "vepMaximumNeighbourhood",
                "vepMean",
                "vepMeanNeighbourhood",
                # other
                "geneCount500kb",
                "proteinGeneCount500kb",
                "credibleSetConfidence",
            ],
        )
