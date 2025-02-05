"""Test L2G Prediction methods."""

import numpy as np
import pandas as pd

from gentropy.dataset.l2g_prediction import L2GPrediction


def test_normalise_feature_contributions() -> None:
    """Tests that scaled probabilities per feature add up to the probability inferred by the model."""
    df = pd.DataFrame(
        {
            "score": [0.45],  # Final probability
            "shap_feature1": [-3.85],
            "shap_feature2": [3.015],
            "shap_feature3": [0.063],
        }
    )
    base_log_odds = 0.56
    scaled_df = L2GPrediction._normalise_feature_contributions(df, base_log_odds)
    reconstructed_prob = (
        scaled_df["scaled_prob_shap_feature1"].sum()
        + scaled_df["scaled_prob_shap_feature2"].sum()
        + scaled_df["scaled_prob_shap_feature3"].sum()
    )
    assert np.allclose(
        reconstructed_prob, df["score"], atol=1e-6
    ), "SHAP probability contributions do not sum to the expected probability."
