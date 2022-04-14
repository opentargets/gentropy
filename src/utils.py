"""
Utilities to perform colocalisation analysis
"""

from pyspark.ml.linalg import VectorUDT, Vectors
import numpy as np


def getLogsum(logABF: VectorUDT):
    """
    This function calculates the log of the sum of the exponentiated
    logs taking out the max, i.e. insuring that the sum is not Inf
    """

    themax = np.max(logABF)
    result = themax + np.log(np.sum(np.exp(logABF - themax)))
    return float(result)


def getPosteriors(allAbfs: VectorUDT):
    """
    Calculates the posterior probability of each hypothesis given the evidence.
    """

    diff = allAbfs - getLogsum(allAbfs)
    abfsPosteriors = np.exp(diff)
    return Vectors.dense(abfsPosteriors)
