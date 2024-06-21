"""Generating variant annotation based on Esembl's Variant Effect Predictor output."""

from __future__ import annotations

import importlib.resources as pkg_resources
import json
from itertools import chain
from typing import TYPE_CHECKING, Dict, List

from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql import types as t

from gentropy.assets import data
from gentropy.common.schemas import parse_spark_schema
from gentropy.common.spark_helpers import (
    enforce_schema,
    order_array_of_structs_by_field,
)
from gentropy.dataset.variant_index import VariantIndex

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame


class VariantEffectPredictorParser:
    """Collection of methods to parse VEP output in json format."""

    # Schema description of the dbXref object:
    DBXREF_SCHEMA = t.ArrayType(
        t.StructType(
            [
                t.StructField("id", t.StringType(), True),
                t.StructField("source", t.StringType(), True),
            ]
        )
    )

    # Schema description of the in silico predictor object:
    IN_SILICO_PREDICTOR_SCHEMA = t.StructType(
        [
            t.StructField("method", t.StringType(), True),
            t.StructField("assessment", t.StringType(), True),
            t.StructField("score", t.FloatType(), True),
            t.StructField("assessmentFlag", t.StringType(), True),
            t.StructField("targetId", t.StringType(), True),
        ]
    )

    @staticmethod
    def get_vep_schema() -> t.StructType:
        """Return the schema of the VEP output.

        Returns:
            t.StructType: VEP output schema.
        """
        return parse_spark_schema("vep_json_output.json")

    @classmethod
    def extract_variant_index_from_vep(
        cls: type[VariantEffectPredictorParser],
        spark: SparkSession,
        vep_output_path: str | list[str],
        **kwargs: bool | float | int | str | None,
    ) -> VariantIndex:
        """Extract variant index from VEP output.

        Args:
            spark (SparkSession): Spark session.
            vep_output_path (str | list[str]): Path to the VEP output.
            **kwargs (bool | float | int | str | None): Additional arguments to pass to spark.read.json.

        Returns:
            VariantIndex: Variant index dataset.

        Raises:
            ValueError: Failed reading file.
            ValueError: The dataset is empty.
        """
        # To speed things up and simplify the json structure, read data following an expected schema:
        vep_schema = cls.get_vep_schema()

        try:
            vep_data = spark.read.json(vep_output_path, schema=vep_schema, **kwargs)
        except ValueError as e:
            raise ValueError(f"Failed reading file: {vep_output_path}.") from e

        if vep_data.isEmpty():
            raise ValueError(f"The dataset is empty: {vep_output_path}")

        # Convert to VariantAnnotation dataset:
        return VariantIndex(
            _df=VariantEffectPredictorParser.process_vep_output(vep_data),
            _schema=VariantIndex.get_schema(),
        )

    @staticmethod
    def _extract_ensembl_xrefs(colocated_variants: Column) -> Column:
        """Build xdbRefs for rs identifiers.

        Args:
            colocated_variants (Column): Colocated variants field from VEP output.

        Returns:
            Column: List of dbXrefs for rs identifiers.
        """
        return VariantEffectPredictorParser._generate_dbxrefs(
            VariantEffectPredictorParser._colocated_variants_to_rsids(
                colocated_variants
            ),
            "ensemblVariation",
        )

    @enforce_schema(DBXREF_SCHEMA)
    @staticmethod
    def _generate_dbxrefs(ids: Column, source: str) -> Column:
        """Convert a list of variant identifiers to dbXrefs given the source label.

        Args:
            ids (Column): List of variant identifiers.
            source (str): Source label for the dbXrefs.

        Returns:
            Column: List of dbXrefs.
        """
        xref_column = f.transform(
            ids, lambda id: f.struct(id.alias("id"), f.lit(source).alias("source"))
        )

        return f.when(xref_column.isNull(), f.array()).otherwise(xref_column)

    @staticmethod
    def _colocated_variants_to_rsids(colocated_variants: Column) -> Column:
        """Extract rs identifiers from the colocated variants VEP field.

        Args:
            colocated_variants (Column): Colocated variants field from VEP output.

        Returns:
            Column: List of rs identifiers.
        """
        return f.when(
            colocated_variants.isNotNull(),
            f.transform(
                colocated_variants, lambda variant: variant.getItem("id").alias("id")
            ),
        ).alias("rsIds")

    @staticmethod
    def _extract_omim_xrefs(colocated_variants: Column) -> Column:
        """Build xdbRefs for OMIM identifiers.

        Args:
            colocated_variants (Column): Colocated variants field from VEP output.

        Returns:
            Column: List of dbXrefs for OMIM identifiers.
        """
        variants_w_omim_ref = f.filter(
            colocated_variants,
            lambda variant: variant.getItem("var_synonyms").getItem("OMIM").isNotNull(),
        )

        omim_var_ids = f.transform(
            variants_w_omim_ref,
            lambda var: f.transform(
                var.getItem("var_synonyms").getItem("OMIM"),
                lambda var: f.regexp_replace(var.cast(t.StringType()), r"\.", r"#"),
            ),
        )

        return VariantEffectPredictorParser._generate_dbxrefs(
            f.flatten(omim_var_ids), "omim"
        )

    @staticmethod
    def _extract_clinvar_xrefs(colocated_variants: Column) -> Column:
        """Build xdbRefs for VCV ClinVar identifiers.

        Args:
            colocated_variants (Column): Colocated variants field from VEP output.

        Returns:
            Column: List of dbXrefs for VCV ClinVar identifiers.
        """
        variants_w_clinvar_ref = f.filter(
            colocated_variants,
            lambda variant: variant.getItem("var_synonyms")
            .getItem("ClinVar")
            .isNotNull(),
        )

        clin_var_ids = f.transform(
            variants_w_clinvar_ref,
            lambda var: f.filter(
                var.getItem("var_synonyms").getItem("ClinVar"),
                lambda x: x.startswith("VCV"),
            ),
        )

        return VariantEffectPredictorParser._generate_dbxrefs(
            f.flatten(clin_var_ids), "clinVar"
        )

    @staticmethod
    def _get_most_severe_transcript(
        transcript_column_name: str, field_name: str
    ) -> Column:
        """Return transcript with the most highest in silico predicted consequence.

        Args:
            transcript_column_name (str): Name of the column containing the list of transcripts.
            field_name (str): Name of the field containing the severity score.

        Returns:
            Column: Most severe transcript.
        """
        # Order transcripts by severity score:
        ordered_transcripts = order_array_of_structs_by_field(
            transcript_column_name, field_name
        )

        # Drop transcripts with no severity score and return the most severe one:
        return f.filter(
            ordered_transcripts,
            lambda transcript: transcript.getItem(field_name).isNotNull(),
        )[0]

    @enforce_schema(IN_SILICO_PREDICTOR_SCHEMA)
    @staticmethod
    def _get_max_alpha_missense(transcripts: Column) -> Column:
        """Return the most severe alpha missense prediction from all transcripts.

        Args:
            transcripts (Column): List of transcripts.

        Returns:
            Column: Most severe alpha missense prediction.
        """
        return f.transform(
            # Extract transcripts with alpha missense values:
            f.filter(
                transcripts,
                lambda transcript: transcript.getItem("alphamissense").isNotNull(),
            ),
            # Extract alpha missense values:
            lambda transcript: f.struct(
                transcript.getItem("alphamissense")
                .getItem("am_pathogenicity")
                .cast(t.FloatType())
                .alias("score"),
                transcript.getItem("alphamissense")
                .getItem("am_class")
                .alias("assessment"),
                f.lit("max alpha missense").alias("method"),
                transcript.getItem("gene_id").alias("targetId"),
            ),
        )[0]

    @enforce_schema(IN_SILICO_PREDICTOR_SCHEMA)
    @staticmethod
    def _vep_in_silico_prediction_extractor(
        transcript_column_name: str,
        method_name: str,
        score_column_name: str | None = None,
        assessment_column_name: str | None = None,
        assessment_flag_column_name: str | None = None,
    ) -> Column:
        """Extract in silico prediction from VEP output.

        Args:
            transcript_column_name (str): Name of the column containing the list of transcripts.
            method_name (str): Name of the in silico predictor.
            score_column_name (str | None): Name of the column containing the score.
            assessment_column_name (str | None): Name of the column containing the assessment.
            assessment_flag_column_name (str | None): Name of the column containing the assessment flag.

        Returns:
            Column: In silico predictor.
        """
        # Get highest score:
        most_severe_transcript: Column = (
            # Getting the most severe transcript:
            VariantEffectPredictorParser._get_most_severe_transcript(
                transcript_column_name, score_column_name
            )
            if score_column_name is not None
            # If we don't have score, just pick one of the transcript where assessment is available:
            else f.filter(
                f.col(transcript_column_name),
                lambda transcript: transcript.getItem(
                    assessment_column_name
                ).isNotNull(),
            )
        )

        return f.when(
            most_severe_transcript.isNotNull(),
            f.struct(
                # Adding method name:
                f.lit(method_name).cast(t.StringType()).alias("method"),
                # Adding assessment:
                f.lit(None).cast(t.StringType()).alias("assessment")
                if assessment_column_name is None
                else most_severe_transcript.getField(assessment_column_name).alias(
                    "assessment"
                ),
                # Adding score:
                f.lit(None).cast(t.FloatType()).alias("score")
                if score_column_name is None
                else most_severe_transcript.getField(score_column_name)
                .cast(t.FloatType())
                .alias("score"),
                # Adding assessment flag:
                f.lit(None).cast(t.StringType()).alias("assessmentFlag")
                if assessment_flag_column_name is None
                else most_severe_transcript.getField(assessment_flag_column_name)
                .cast(t.FloatType())
                .alias("assessmentFlag"),
                # Adding target id if present:
                most_severe_transcript.getItem("gene_id").alias("targetId"),
            ),
        )

    @staticmethod
    def _parser_amino_acid_change(amino_acids: Column, protein_end: Column) -> Column:
        """Convert VEP amino acid change information to one letter code aa substitution code.

        Args:
            amino_acids (Column): Amino acid change information.
            protein_end (Column): Protein end information.

        Returns:
            Column: Amino acid change in one letter code.
        """
        return f.when(
            amino_acids.isNotNull(),
            f.concat(
                f.split(amino_acids, r"\/")[0],
                protein_end,
                f.split(amino_acids, r"\/")[1],
            ),
        ).otherwise(f.lit(None))

    @staticmethod
    def _collect_uniprot_accessions(trembl: Column, swissprot: Column) -> Column:
        """Flatten arrays containing Uniprot accessions.

        Args:
            trembl (Column): List of TrEMBL protein accessions.
            swissprot (Column): List of SwissProt protein accessions.

        Returns:
            Column: List of unique Uniprot accessions extracted from swissprot and trembl arrays, splitted from version numbers.
        """
        return f.array_distinct(
            f.transform(
                f.flatten(
                    f.filter(f.array(trembl, swissprot), lambda x: x.isNotNull())
                ),
                lambda x: f.split(x, r"\.")[0],
            )
        )

    @staticmethod
    def _consequence_to_sequence_ontology(
        col: Column, so_dict: Dict[str, str]
    ) -> Column:
        """Convert VEP consequence terms to sequence ontology identifiers.

        Args:
            col (Column): Column containing VEP consequence terms.
            so_dict (Dict[str, str]): Dictionary mapping VEP consequence terms to sequence ontology identifiers.

        Returns:
            Column: Column containing sequence ontology identifiers.
        """
        map_expr = f.create_map(*[f.lit(x) for x in chain(*so_dict.items())])

        return map_expr[col].alias("ancestry")

    @staticmethod
    def _parse_variant_id(variant_id: Column) -> List[Column]:
        """Parse variant identifier into chromosome, position, reference allele and alternate allele.

        Args:
            variant_id (Column): Column containing variant identifier.

        Returns:
            List[Column]: List of columns containing chromosome, position, reference allele and alternate allele.
        """
        variant_fields = f.split(variant_id, r"_")

        return [
            variant_fields[0].cast(t.StringType()).alias("chromosome"),
            variant_fields[1].cast(t.IntegerType()).alias("position"),
            variant_fields[2].cast(t.StringType()).alias("referenceAllele"),
            variant_fields[3].cast(t.StringType()).alias("alternateAllele"),
        ]

    @classmethod
    def process_vep_output(cls, vep_output: DataFrame) -> DataFrame:
        """Process and format a VEP output in JSON format.

        Args:
            vep_output (DataFrame): raw VEP output, read as spark DataFrame.

        Returns:
           DataFrame: processed data in the right shape.
        """
        # Reading consequence to sequence ontology map:
        sequence_ontology_map = json.loads(
            pkg_resources.read_text(data, "so_mappings.json", encoding="utf-8")
        )
        # Processing VEP output:
        return (
            vep_output
            # Dropping non-canonical transcript consequences:  # TODO: parametrize this.
            .withColumn(
                "transcript_consequences",
                f.filter(
                    f.col("transcript_consequences"),
                    lambda consequence: consequence.getItem("canonical") == 1,
                ),
            )
            .select(
                # Extracting variant identifier:
                f.col("id").alias("variantId"),
                # Parse chr:pos:alt:ref:
                *cls._parse_variant_id(f.col("id")),
                # Extracting corss_references from colocated variants:
                cls._extract_ensembl_xrefs(f.col("colocated_variants")).alias(
                    "ensembl_xrefs"
                ),
                cls._extract_omim_xrefs(f.col("colocated_variants")).alias(
                    "omim_xrefs"
                ),
                cls._extract_clinvar_xrefs(f.col("colocated_variants")).alias(
                    "clinvar_xrefs"
                ),
                # Extracting in silico predictors
                f.when(
                    # The following in-silico predictors are only available for variants with transcript consequences:
                    f.col("transcript_consequences").isNotNull(),
                    f.filter(
                        f.array(
                            # Extract CADD scores:
                            cls._vep_in_silico_prediction_extractor(
                                transcript_column_name="transcript_consequences",
                                method_name="phred scaled CADD",
                                score_column_name="cadd_phred",
                            ),
                            # Extract polyphen scores:
                            cls._vep_in_silico_prediction_extractor(
                                transcript_column_name="transcript_consequences",
                                method_name="polyphen",
                                score_column_name="polyphen_score",
                                assessment_column_name="polyphen_prediction",
                            ),
                            # Extract sift scores:
                            cls._vep_in_silico_prediction_extractor(
                                transcript_column_name="transcript_consequences",
                                method_name="sift",
                                score_column_name="sift_score",
                                assessment_column_name="sift_prediction",
                            ),
                            # Extract loftee scores:
                            cls._vep_in_silico_prediction_extractor(
                                method_name="loftee",
                                transcript_column_name="transcript_consequences",
                                score_column_name="lof",
                                assessment_column_name="lof",
                                assessment_flag_column_name="lof_filter",
                            ),
                            # Extract max alpha missense:
                            cls._get_max_alpha_missense(
                                f.col("transcript_consequences")
                            ),
                        ),
                        lambda predictor: predictor.isNotNull(),
                    ),
                )
                .otherwise(
                    # Extract CADD scores from intergenic object:
                    f.array(
                        cls._vep_in_silico_prediction_extractor(
                            transcript_column_name="intergenic_consequences",
                            method_name="phred scaled CADD",
                            score_column_name="cadd_phred",
                        ),
                    )
                )
                .alias("inSilicoPredictors"),
                # Convert consequence to SO:
                cls._consequence_to_sequence_ontology(
                    f.col("most_severe_consequence"), sequence_ontology_map
                ).alias("mostSevereConsequenceId"),
                # Collect transcript consequence:
                f.when(
                    f.col("transcript_consequences").isNotNull(),
                    f.transform(
                        f.col("transcript_consequences"),
                        lambda transcript: f.struct(
                            # Convert consequence terms to SO identifier:
                            f.transform(
                                transcript.consequence_terms,
                                lambda y: cls._consequence_to_sequence_ontology(
                                    y, sequence_ontology_map
                                ),
                            ).alias("variantFunctionalConsequenceIds"),
                            # Format amino acid change:
                            cls._parser_amino_acid_change(
                                transcript.amino_acids, transcript.protein_end
                            ).alias("amino_acid_change"),
                            # Extract and clean uniprot identifiers:
                            cls._collect_uniprot_accessions(
                                transcript.swissprot,
                                transcript.trembl,
                            ).alias("uniprotAccessions"),
                            # Add canonical flag:
                            f.when(transcript.canonical == 1, f.lit(True))
                            .otherwise(f.lit(False))
                            .alias("isEnsemblCanonical"),
                            # Extract other fields as is:
                            transcript.codons.alias("codons"),
                            transcript.distance.alias("distance"),
                            transcript.gene_id.alias("targetId"),
                            transcript.impact.alias("impact"),
                            transcript.transcript_id.alias("transcriptId"),
                            transcript.lof.cast(t.StringType()).alias(
                                "lofteePrediction"
                            ),
                            transcript.lof.cast(t.FloatType()).alias("siftPrediction"),
                            transcript.lof.cast(t.FloatType()).alias(
                                "polyphenPrediction"
                            ),
                        ),
                    ),
                ).alias("transcriptConsequences"),
                # Extracting rsids:
                cls._colocated_variants_to_rsids(f.col("colocated_variants")).alias(
                    "rsIds"
                ),
            )
            # Adding protvar xref for missense variants:  # TODO: making and extendable list of consequences
            .withColumn(
                "protvar_xrefs",
                f.when(
                    f.size(
                        f.filter(
                            f.col("transcriptConsequences"),
                            lambda x: f.array_contains(
                                x.variantFunctionalConsequenceIds, "SO_0001583"
                            ),
                        )
                    )
                    > 0,
                    cls._generate_dbxrefs(f.array(f.col("variantId")), "protVar"),
                ),
            )
            .withColumn(
                "dbXrefs",
                f.flatten(
                    f.filter(
                        f.array(
                            f.col("ensembl_xrefs"),
                            f.col("omim_xrefs"),
                            f.col("clinvar_xrefs"),
                            f.col("protvar_xrefs"),
                        ),
                        lambda x: x.isNotNull(),
                    )
                ),
            )
            # Dropping intermediate xref columns:
            .drop(*["ensembl_xrefs", "omim_xrefs", "clinvar_xrefs", "protvar_xrefs"])
        )
