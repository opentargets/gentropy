"""Dataset describing single point association."""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyspark.sql.functions as f

from otg.common.utils import split_pvalue
from otg.dataset.dataset import Dataset

if TYPE_CHECKING:
    from otg.dataset.genomic_region import GenomicRegion


@dataclass
class SinglePointAssociation(Dataset):
    """This class contains methods applicable to single point associations."""

    def p_value_filter(
        self: SinglePointAssociation, p_value_threshold: float
    ) -> SinglePointAssociation:
        """Filter SinglePointAssociation dataset on the provided p-value threshold, returns same type.

        Args:
            p_value_threshold (float): upper limit of the p-value to be filtered upon.

        Returns:
            SinglePointAssociation: summary statistics object containing single point associations with p-values at least as significant as the provided threshold.
        """
        # Converting p-value to mantissa and exponent:
        (mantissa, exponent) = split_pvalue(p_value_threshold)

        # Applying filter:
        df = self._df.filter(
            (f.col("pValueExponent") < exponent)
            | (
                (f.col("pValueExponent") == exponent)
                & (f.col("pValueMantissa") <= mantissa)
            )
        )
        return type(self)(_df=df, _schema=self._schema)

    def filter_by_region(
        self: SinglePointAssociation, region: GenomicRegion, exlude_region: bool = False
    ) -> SinglePointAssociation:
        """Filter single-point associations by genomic region.

        Args:
            region (GenomicRegion): Region which will be filter on or excluded from the dataset.
            exlude_region (bool, optional): Flags indicating if the genomic region is filtered or exclude from dataset.
                Defaults to False.

        Returns:
            SinglePointAssociation: Filtered dataset based on the provided genomic region.
        """
        # Constructing filter expression:
        filter_expression = (
            (f.col("chromosome") == region.chromosome)
            & (f.col("position") >= region.start)
            & (f.col("position") <= region.end)
        )

        # If regomic region needs to be excluded:
        if exlude_region:
            filter_expression = ~filter_expression

        # Applying filter:
        filtered_df = self.df.filter(filter_expression)

        # Returning the dataset:
        return type(self)(_df=filtered_df, _schema=self.schema)
