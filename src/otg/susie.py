"""Step to run study locus fine-mapping."""

from __future__ import annotations

from dataclasses import dataclass

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

import numpy as np
import pandas as pd
import pyspark.sql.functions as f
import scipy.linalg
import scipy.special

from otg.common.session import Session
from otg.config import SuSiEStepConfig
from otg.method.susie import SuSiE


@dataclass
class SuSiEStep(SuSiEStepConfig):
     """SuSiE Fine-mapping for an input locus
     untested code"""

    session: Session = Session()

    def run_susie_finemapping(
        z=z,
        filtered_LDMatrix=filtered_LDMatrix,
        n_sample=500000,
    ):
        """Runs the SuSiE fine-mapping."""
        eigenvals, V = scipy.linalg.eigh(filtered_LDMatrix)
        pi0 = None
        susie_output = susie(
            z=z,
            meansq=1,
            n=n_sample,
            L=10,
            LD=filtered_LDMatrix,
            V=None,
            Dsq=None,
            est_ssq=True,
            ssq=None,
            ssq_range=(0, 1),
            pi0=None,
            est_sigmasq=True,
            est_tausq=True,
            sigmasq=1,
            tausq=0,
            method="moments",
            sigmasq_range=None,
            tausq_range=None,
            PIP=None,
            mu=None,
            maxiter=100,
            PIP_tol=1e-3,
            verbose=True,
        )

        susie_output["cred"] = cred(
            susie_output["PIP"],
            coverage=0.9,
            purity=0.1,
            LD=filtered_LDMatrix,
            V=V,
            Dsq=None,
            n=n_sample,
        )
        return susie_output

    def write_tsv(df, out_file):
        """Write tsv file of SuSiE results."""
        df.to_csv(out_file, sep="\t", index=False)

    def process_output(output_dict, df, output_prefix):
        """Organise SuSiE results as output."""
        df["prob"] = 1 - (1 - output_dict["PIP"]).prod(axis=1)
        L = output_dict["PIP"].shape[1]
        alpha_cols = ["alpha{}".format(i) for i in range(1, L + 1)]
        mu_cols = ["mu{}".format(i) for i in range(1, L + 1)]
        df[
            alpha_cols
            + mu_cols
            + ["omega{}".format(i) for i in range(1, L + 1)]
            + ["lbf_variable{}".format(i) for i in range(1, L + 1)]
        ] = pd.DataFrame(
            np.concatenate(
                (
                    output_dict["PIP"],
                    output_dict["mu"],
                    output_dict["omega"],
                    output_dict["lbf_variable"],
                ),
                axis=1,
            )
        )
        df["alpha"] = output_dict["alpha"]
        df["post_mean"] = (
            np.sum(df[mu_cols].values * df[alpha_cols].values, axis=1) + df["alpha"]
        )
        df["tausq"] = output_dict["tausq"]
        df["sigmasq"] = output_dict["sigmasq"]
        df["cs"] = -1
        if len(output_dict["cred"]) > 0:
            df = df.reset_index(drop=True)
            for i, x in enumerate(output_dict["cred"]):
                df.loc[x, "cs"] = i + 1
        out_file = output_prefix + ".susieinf.bgz"
        write_tsv(df, out_file)
