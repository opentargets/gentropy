"""Variant index: one row per variant, with its annotation.

The index is built outside gentropy, from VEP and other annotation sources. Each
variant appears once, keyed by ``variantId``. The other datasets key their
variants the same way, so they can look up a variant's annotation directly.
``chromosome`` and ``position`` locate the variant, and the two alleles complete
its description.

The annotation columns are optional. ``transcriptConsequences`` holds the
predicted consequence of the variant on each transcript it falls near, including
its distance to the gene. ``variantEffect`` holds the effect assessments of
predictors such as VEP, LOFTEE or FoldX, each with a normalised score.
``alleleFrequencies`` gives the allele frequency in each population.

The layout is the legacy one, and nested fields keep their legacy nullability.

Examples:
    >>> VariantIndex.print_schema()
    VariantIndex
     |-- variantId: string (mandatory: HasVariantId.variant_id)
     |-- chromosome: string (mandatory: VariantLocation.chromosome)
     |-- position: int (mandatory: VariantLocation.position)
     |-- referenceAllele: string (mandatory)
     |-- alternateAllele: string (mandatory)
     |-- variantEffect: array (optional)
     |    |-- element: struct
     |    |    |-- method: string
     |    |    |-- assessment: string
     |    |    |-- score: float
     |    |    |-- assessmentFlag: string
     |    |    |-- targetId: string
     |    |    |-- normalisedScore: double
     |-- mostSevereConsequenceId: string (optional)
     |-- transcriptConsequences: array (optional)
     |    |-- element: struct
     |    |    |-- variantFunctionalConsequenceIds: array<string>
     |    |    |-- aminoAcidChange: string
     |    |    |-- uniprotAccessions: array<string>
     |    |    |-- isEnsemblCanonical: boolean
     |    |    |-- codons: string
     |    |    |-- distanceFromFootprint: bigint
     |    |    |-- distanceFromTss: bigint
     |    |    |-- appris: string
     |    |    |-- maneSelect: string
     |    |    |-- targetId: string
     |    |    |-- impact: string
     |    |    |-- lofteePrediction: string
     |    |    |-- siftPrediction: float
     |    |    |-- polyphenPrediction: float
     |    |    |-- consequenceScore: float
     |    |    |-- transcriptIndex: int
     |    |    |-- approvedSymbol: string
     |    |    |-- biotype: string
     |    |    |-- transcriptId: string
     |-- rsIds: array<string> (optional)
     |-- hgvsId: string (optional)
     |-- alleleFrequencies: array (optional)
     |    |-- element: struct
     |    |    |-- populationName: string
     |    |    |-- alleleFrequency: double
     |-- dbXrefs: array (optional)
     |    |-- element: struct
     |    |    |-- id: string
     |    |    |-- source: string
     |-- variantDescription: string (optional)

    A variant described by its mandatory columns and its most severe consequence:

    >>> variants = VariantIndex(
    ...     spark.createDataFrame(
    ...         [("1_100_A_T", "1", 100, "A", "T", "SO_0001583")],
    ...         "variantId string, chromosome string, position int, "
    ...         "referenceAllele string, alternateAllele string, "
    ...         "mostSevereConsequenceId string",
    ...     )
    ... )
    >>> variants.df.select(
    ...     variants.variant_id, variants.most_severe_consequence_id
    ... ).show()
    +---------+-----------------------+
    |variantId|mostSevereConsequenceId|
    +---------+-----------------------+
    |1_100_A_T|             SO_0001583|
    +---------+-----------------------+
    <BLANKLINE>
"""

from __future__ import annotations

from pyspark.sql import types as t

from gentropy.model.core.dataset import Dataset
from gentropy.model.core.schema import implements, optional, required
from gentropy.model.trait.identifier import HasVariantId
from gentropy.model.trait.locus import VariantLocation


class VariantIndex(Dataset[HasVariantId | VariantLocation]):
    """One row per variant, with its annotation."""

    variant_id = implements(HasVariantId.variant_id)
    chromosome = implements(VariantLocation.chromosome)
    position = implements(VariantLocation.position)
    reference_allele = required(t.StringType())
    alternate_allele = required(t.StringType())
    variant_effect = optional(
        t.ArrayType(
            t.StructType(
                [
                    t.StructField("method", t.StringType()),
                    t.StructField("assessment", t.StringType()),
                    t.StructField("score", t.FloatType()),
                    t.StructField("assessmentFlag", t.StringType()),
                    t.StructField("targetId", t.StringType()),
                    t.StructField("normalisedScore", t.DoubleType()),
                ]
            )
        )
    )
    most_severe_consequence_id = optional(t.StringType())
    transcript_consequences = optional(
        t.ArrayType(
            t.StructType(
                [
                    t.StructField(
                        "variantFunctionalConsequenceIds", t.ArrayType(t.StringType())
                    ),
                    t.StructField("aminoAcidChange", t.StringType()),
                    t.StructField("uniprotAccessions", t.ArrayType(t.StringType())),
                    t.StructField(
                        "isEnsemblCanonical", t.BooleanType(), nullable=False
                    ),
                    t.StructField("codons", t.StringType()),
                    t.StructField("distanceFromFootprint", t.LongType()),
                    t.StructField("distanceFromTss", t.LongType()),
                    t.StructField("appris", t.StringType()),
                    t.StructField("maneSelect", t.StringType()),
                    t.StructField("targetId", t.StringType()),
                    t.StructField("impact", t.StringType()),
                    t.StructField("lofteePrediction", t.StringType()),
                    t.StructField("siftPrediction", t.FloatType()),
                    t.StructField("polyphenPrediction", t.FloatType()),
                    t.StructField("consequenceScore", t.FloatType()),
                    t.StructField("transcriptIndex", t.IntegerType()),
                    t.StructField("approvedSymbol", t.StringType()),
                    t.StructField("biotype", t.StringType()),
                    t.StructField("transcriptId", t.StringType()),
                ]
            ),
            containsNull=False,
        )
    )
    rs_ids = optional(t.ArrayType(t.StringType()))
    hgvs_id = optional(t.StringType())
    allele_frequencies = optional(
        t.ArrayType(
            t.StructType(
                [
                    t.StructField("populationName", t.StringType()),
                    t.StructField("alleleFrequency", t.DoubleType()),
                ]
            )
        )
    )
    db_xrefs = optional(
        t.ArrayType(
            t.StructType(
                [
                    t.StructField("id", t.StringType()),
                    t.StructField("source", t.StringType()),
                ]
            )
        )
    )
    variant_description = optional(t.StringType())
