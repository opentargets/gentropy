"""Test study locus dataset."""

from __future__ import annotations

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    StringType,
    StructField,
    StructType,
)

# Used primarily for test_unique_variants_in_locus but also for other tests
test_unique_variants_in_locus_test_data = [
    (
        # Locus is not null, should return union between variants in locus and lead variant
        [
            (
                "1",
                "traitA",
                "22_varA",
                [
                    {"variantId": "22_varA", "posteriorProbability": 0.44},
                    {"variantId": "22_varB", "posteriorProbability": 0.015},
                ],
            ),
        ],
        [
            (
                "22_varA",
                "22",
            ),
            (
                "22_varB",
                "22",
            ),
        ],
    ),
    (
        # locus is null, should return lead variant
        [
            ("1", "traitA", "22_varA", None),
        ],
        [
            (
                "22_varA",
                "22",
            ),
        ],
    ),
]

test_unique_variants_in_locus_test_schema = StructType(
    [
        StructField("studyLocusId", StringType(), True),
        StructField("studyId", StringType(), True),
        StructField("variantId", StringType(), True),
        StructField(
            "locus",
            ArrayType(
                StructType(
                    [
                        StructField("variantId", StringType(), True),
                        StructField("posteriorProbability", DoubleType(), True),
                    ]
                )
            ),
            True,
        ),
    ]
)


# Used primarily for test_annotate_credible_sets but also for other tests
test_annotate_credible_sets_test_data = [
    (
        # Simple case
        [
            # Observed
            (
                "1",
                "traitA",
                "leadB",
                [{"variantId": "tagVariantA", "posteriorProbability": 1.0}],
            ),
        ],
        [
            # Expected
            (
                "1",
                "traitA",
                "leadB",
                [
                    {
                        "variantId": "tagVariantA",
                        "posteriorProbability": 1.0,
                        "is95CredibleSet": True,
                        "is99CredibleSet": True,
                    }
                ],
            )
        ],
    ),
    (
        # Unordered credible set
        [
            # Observed
            (
                "1",
                "traitA",
                "leadA",
                [
                    {"variantId": "tagVariantA", "posteriorProbability": 0.44},
                    {"variantId": "tagVariantB", "posteriorProbability": 0.015},
                    {"variantId": "tagVariantC", "posteriorProbability": 0.04},
                    {"variantId": "tagVariantD", "posteriorProbability": 0.005},
                    {"variantId": "tagVariantE", "posteriorProbability": 0.5},
                    {"variantId": "tagVariantNull", "posteriorProbability": None},
                    {"variantId": "tagVariantNull", "posteriorProbability": None},
                ],
            )
        ],
        [
            # Expected
            (
                "1",
                "traitA",
                "leadA",
                [
                    {
                        "variantId": "tagVariantE",
                        "posteriorProbability": 0.5,
                        "is95CredibleSet": True,
                        "is99CredibleSet": True,
                    },
                    {
                        "variantId": "tagVariantA",
                        "posteriorProbability": 0.44,
                        "is95CredibleSet": True,
                        "is99CredibleSet": True,
                    },
                    {
                        "variantId": "tagVariantC",
                        "posteriorProbability": 0.04,
                        "is95CredibleSet": True,
                        "is99CredibleSet": True,
                    },
                    {
                        "variantId": "tagVariantB",
                        "posteriorProbability": 0.015,
                        "is95CredibleSet": False,
                        "is99CredibleSet": True,
                    },
                    {
                        "variantId": "tagVariantD",
                        "posteriorProbability": 0.005,
                        "is95CredibleSet": False,
                        "is99CredibleSet": False,
                    },
                    {
                        "variantId": "tagVariantNull",
                        "posteriorProbability": None,
                        "is95CredibleSet": False,
                        "is99CredibleSet": False,
                    },
                    {
                        "variantId": "tagVariantNull",
                        "posteriorProbability": None,
                        "is95CredibleSet": False,
                        "is99CredibleSet": False,
                    },
                ],
            )
        ],
    ),
    (
        # Null credible set
        [
            # Observed
            (
                "1",
                "traitA",
                "leadB",
                None,
            ),
        ],
        [
            # Expected
            (
                "1",
                "traitA",
                "leadB",
                None,
            )
        ],
    ),
    (
        # Empty credible set
        [
            # Observed
            (
                "1",
                "traitA",
                "leadB",
                [],
            ),
        ],
        [
            # Expected
            (
                "1",
                "traitA",
                "leadB",
                None,
            )
        ],
    ),
]
test_annotate_credible_sets_test_schema = StructType(
    [
        StructField("studyLocusId", StringType(), True),
        StructField("studyId", StringType(), True),
        StructField("variantId", StringType(), True),
        StructField(
            "locus",
            ArrayType(
                StructType(
                    [
                        StructField("variantId", StringType(), True),
                        StructField("posteriorProbability", DoubleType(), True),
                        StructField("is95CredibleSet", BooleanType(), True),
                        StructField("is99CredibleSet", BooleanType(), True),
                    ]
                )
            ),
            True,
        ),
    ]
)
