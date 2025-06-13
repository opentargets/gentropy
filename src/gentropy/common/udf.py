"""Common UDF functions.

The functions in this module are designed to be run as PySpark UDFs with parallel execution provided by pandas and numpy.

Note:
    The decorated function(s) in this module with signature f(pd.Series) -> pd.Series has to be annotated with the functionType
    parameter, as it is currently only way to distinguish between many -> many and one -> one UDFs.
    The API is not consistent between different pyspark versions, next versions may deprecate the functionType parameter in advance of
    python type hints. See - https://issues.apache.org/jira/browse/SPARK-28264
    and the draft for new API - https://docs.google.com/document/d/1-kV0FS_LF2zvaRh_GhkV32Uqksm_Sq8SvnBBmRyxm30/edit?tab=t.0.
"""

import numpy as np
import pandas as pd
from pyspark.sql import types as t
from pyspark.sql.pandas.functions import PandasUDFType, pandas_udf
from scipy.stats import chi2


@pandas_udf(returnType=t.DoubleType(), functionType=PandasUDFType.SCALAR)
def chi2_inverse_survival_function(x: pd.Series) -> pd.Series:
    """Calculate the inverse survival function of the chi2 distribution with 1 degree of freedom.

    Args:
        x (pd.Series): A pandas Series containing p-values (between 0 and 1).

    Returns:
        pd.Series: A pandas Series containing the chi2 statistic corresponding to the input p-values.

    Get the chi2 statistic for a given p-value (x).

    Examples:
        >>> data = [(0.1,), (0.05,), (0.001,)]
        >>> schema = "pValue Float"
        >>> df = spark.createDataFrame(data, schema=schema)
        >>> df.show()
        +------+
        |pValue|
        +------+
        |   0.1|
        |  0.05|
        | 0.001|
        +------+
        <BLANKLINE>

        >>> import pyspark.sql.functions as f
        >>> chi2 = f.round(chi2_inverse_survival_function("pValue"), 2).alias("chi2_stat")
        >>> df.select("pValue", chi2).show()
        +------+---------+
        |pValue|chi2_stat|
        +------+---------+
        |   0.1|     2.71|
        |  0.05|     3.84|
        | 0.001|    10.83|
        +------+---------+
        <BLANKLINE>
    """
    return pd.Series(chi2.isf(x, df=1).astype(np.float64))


@pandas_udf(returnType=t.DoubleType(), functionType=PandasUDFType.SCALAR)
def chi2_survival_function(x: pd.Series) -> pd.Series:
    """Calculate the survival function of the chi2 distribution.

    Args:
        x (pd.Series): A pandas Series containing chi2 statistics or z-scores squared.

    Returns:
        pd.Series: A pandas Series containing the p-values corresponding to the input chi2 statistics or z-scores squared.

    Useful to convert the z-score^2 or chi2 statistic to a p-value.

    Examples:
        >>> data = [(1.0, 1.0), (-1.0, 1.0), (10.0, 100.0)]
        >>> schema = "zScore Float, chi2 Float"
        >>> df = spark.createDataFrame(data, schema=schema)
        >>> df.show()
        +------+-----+
        |zScore| chi2|
        +------+-----+
        |   1.0|  1.0|
        |  -1.0|  1.0|
        |  10.0|100.0|
        +------+-----+
        <BLANKLINE>

        >>> import pyspark.sql.functions as f
        >>> pval_from_z2 = f.round(chi2_survival_function(f.col("zScore")**2), 2).alias("pValueZscore^2")
        >>> pval_from_chi2 = f.round(chi2_survival_function("chi2"), 2).alias("pValueChi2")
        >>> df.select("zScore", pval_from_z2, "chi2", pval_from_chi2).show()
        +------+--------------+-----+----------+
        |zScore|pValueZscore^2| chi2|pValueChi2|
        +------+--------------+-----+----------+
        |   1.0|          0.32|  1.0|      0.32|
        |  -1.0|          0.32|  1.0|      0.32|
        |  10.0|           0.0|100.0|       0.0|
        +------+--------------+-----+----------+
        <BLANKLINE>
    """
    return pd.Series(chi2.sf(x, df=1).astype(np.float64))
