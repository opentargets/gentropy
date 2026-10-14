"""Traits describing a fine-mapping result.

``HasFineMappingMethod`` holds the method that produced a credible set, such as
``SuSiE-inf`` or ``PICS``; it is part of the credible set key, so two methods
picking the same lead in one locus give two credible sets.
``PosteriorProbability`` holds the posterior probability that a variant is
causal for the signal. Every fine-mapping method assigns one to every variant
it returns, which is why a credible set variant always has it.

Example:
    >>> HasFineMappingMethod.fine_mapping_method.name
    'fineMappingMethod'
    >>> PosteriorProbability.posterior_probability.dtype.simpleString()
    'float'
"""

from __future__ import annotations

from pyspark.sql import types as t

from gentropy.model.core.trait import Field, Trait


class HasFineMappingMethod(Trait):
    """Holds the fine-mapping method that produced a credible set."""

    fine_mapping_method = Field(t.StringType())


class PosteriorProbability(Trait):
    """Holds the posterior probability that a variant is causal."""

    posterior_probability = Field(t.FloatType())
