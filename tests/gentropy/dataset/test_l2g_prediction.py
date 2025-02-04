"""Test L2G Prediction methods."""

import numpy as np
import pandas as pd

from gentropy.dataset.l2g_prediction import L2GPrediction


def test_normalise_feature_contributions() -> None:
    """Tests that scaled probabilities per feature add up to the probability inferred by the model."""
    df = pd.DataFrame(
        {
            "score": [0.163311],  # Final probability
            "shap_feature1": [-3.850356],
            "shap_feature2": [3.015085],
            "shap_feature3": [0.063206],
        }
    )
    scaled_df = L2GPrediction._normalise_feature_contributions(df)
    reconstructed_prob = (
        scaled_df["scaled_prob_shap_feature1"].sum()
        + scaled_df["scaled_prob_shap_feature2"].sum()
        + scaled_df["scaled_prob_shap_feature3"].sum()
    )
    assert np.allclose(
        reconstructed_prob, df["score"], atol=1e-6
    ), "SHAP probability contributions do not sum to the expected probability."
