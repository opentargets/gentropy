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
        filtered_LDMatrix: DataFrame,
        filtered_StudyLocus: DataFrame,
        n_sample=int,
    ):
        """Runs the SuSiE fine-mapping."""
        z = filtered_StudyLocus["z"]
        LD = np.loadtxt(filtered_LDMatrix)
        eigenvals, V = scipy.linalg.eigh(LD)
        pi0 = None
        susie_output = susie(
            z=filtered_StudyLocus["z"],
            n_sample,
            LD=filtered_LDMatrix,
            V=V,
            Dsq=Dsq,
            est_ssq=True,
            ssq=None,
            ssq_range=(0, 1),
            pi0=pi0,
            method='MLE' #decides whether to run MoM or MLE function
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
            LD=None,
            V=V,
            Dsq=Dsq,
            n=n_sample,
        )
        return susie_output

    def process_output(method_name, output_dict, df, output_prefix):
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
        logging.info("Saving output to %s" % (out_file))
        write_bgz(df, out_file)
