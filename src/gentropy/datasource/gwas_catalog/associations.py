"""Study Locus for GWAS Catalog data source."""

from __future__ import annotations

import importlib.resources as pkg_resources
import json
from dataclasses import dataclass
from itertools import chain
from typing import TYPE_CHECKING, cast

import pyspark.sql.functions as f
from pyspark.sql.types import DoubleType, FloatType, IntegerType, StringType
from pyspark.sql.window import Window

from gentropy.assets import data
from gentropy.common.processing import parse_efos
from gentropy.common.spark import get_record_with_maximum_value
from gentropy.common.stats import normalise_gwas_statistics, pvalue_from_neglogpval
from gentropy.common.types import PValComponents
from gentropy.config import WindowBasedClumpingStepConfig
from gentropy.dataset.study_locus import StudyLocus, StudyLocusQualityCheck

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame

    from gentropy.dataset.variant_index import VariantIndex


@dataclass
class GWASCatalogCuratedAssociationsParser:
    """GWAS Catalog curated associations parser."""

    @staticmethod
    def convert_gnomad_position_to_ensembl(
        position: Column, reference: Column, alternate: Column
    ) -> Column:
        """Convert GnomAD variant position to Ensembl variant position.

        For indels (the reference or alternate allele is longer than 1), then adding 1 to the position, for SNPs,
        the position is unchanged. More info about the problem: https://www.biostars.org/p/84686/

        Args:
            position (Column): Position of the variant in GnomAD's coordinates system.
            reference (Column): The reference allele in GnomAD's coordinates system.
            alternate (Column): The alternate allele in GnomAD's coordinates system.

        Returns:
            Column: The position of the variant in the Ensembl genome.

        Examples:
            >>> d = [(1, "A", "C"), (2, "AA", "C"), (3, "A", "AA")]
            >>> df = spark.createDataFrame(d).toDF("position", "reference", "alternate")
            >>> df.withColumn("new_position", GWASCatalogCuratedAssociationsParser.convert_gnomad_position_to_ensembl(f.col("position"), f.col("reference"), f.col("alternate"))).show()
            +--------+---------+---------+------------+
            |position|reference|alternate|new_position|
            +--------+---------+---------+------------+
            |       1|        A|        C|           1|
            |       2|       AA|        C|           3|
            |       3|        A|       AA|           4|
            +--------+---------+---------+------------+
            <BLANKLINE>
        """
        return f.when(
            (f.length(reference) > 1) | (f.length(alternate) > 1), position + 1
        ).otherwise(position)

    @staticmethod
    def _split_pvalue_column(pvalue: Column) -> PValComponents:
        """Parse p-value column.

        Args:
            pvalue (Column): p-value [string]

        Returns:
            PValComponents: p-value mantissa and exponent

        Example:
            >>> import pyspark.sql.types as t
            >>> d = [("1.0"), ("0.5"), ("1E-20"), ("3E-3"), ("1E-1000")]
            >>> df = spark.createDataFrame(d, t.StringType())
            >>> df.select('value',*GWASCatalogCuratedAssociationsParser._split_pvalue_column(f.col('value'))).show()
            +-------+--------------+--------------+
            |  value|pValueMantissa|pValueExponent|
            +-------+--------------+--------------+
            |    1.0|           1.0|             1|
            |    0.5|           0.5|             1|
            |  1E-20|           1.0|           -20|
            |   3E-3|           3.0|            -3|
            |1E-1000|           1.0|         -1000|
            +-------+--------------+--------------+
            <BLANKLINE>

        """
        split = f.split(pvalue, "E")
        return PValComponents(
            mantissa=split.getItem(0).cast("float").alias("pValueMantissa"),
            exponent=f.coalesce(split.getItem(1).cast("integer"), f.lit(1)).alias(
                "pValueExponent"
            ),
        )

    @staticmethod
    def _normalise_pvaluetext(p_value_text: Column) -> Column:
        """Normalised p-value text column to a standardised format.

        For cases where there is no mapping, the value is set to null.

        Args:
            p_value_text (Column): `pValueText` column from GWASCatalog

        Returns:
            Column: Array column after using GWAS Catalog mappings. There might be multiple mappings for a single p-value text.

        Example:
            >>> import pyspark.sql.types as t
            >>> d = [("European Ancestry"), ("African ancestry"), ("Alzheimer’s Disease"), ("(progression)"), (""), (None)]
            >>> df = spark.createDataFrame(d, t.StringType())
            >>> df.withColumn('normalised', GWASCatalogCuratedAssociationsParser._normalise_pvaluetext(f.col('value'))).show()
            +-------------------+----------+
            |              value|normalised|
            +-------------------+----------+
            |  European Ancestry|      [EA]|
            |   African ancestry|      [AA]|
            |Alzheimer’s Disease|      [AD]|
            |      (progression)|      NULL|
            |                   |      NULL|
            |               NULL|      NULL|
            +-------------------+----------+
            <BLANKLINE>

        """
        # GWAS Catalog to p-value mapping
        pkg = pkg_resources.files(data).joinpath("gwas_pValueText_map.json")
        with pkg.open(encoding="utf-8") as file:
            json_dict = json.load(file)
            json_dict = cast(dict[str, str], json_dict)

        map_expr = f.create_map(*[f.lit(x) for x in chain(*json_dict.items())])

        splitted_col = f.split(f.regexp_replace(p_value_text, r"[\(\)]", ""), ",")
        mapped_col = f.transform(splitted_col, lambda x: map_expr[x])
        return f.when(f.forall(mapped_col, lambda x: x.isNull()), None).otherwise(
            mapped_col
        )

    @staticmethod
    def _extract_risk_allele(risk_allele: Column) -> Column:
        """Extract risk allele from provided "STRONGEST SNP-RISK ALLELE" input column.

        If multiple risk alleles are present, the first one is returned.

        Args:
            risk_allele (Column): `riskAllele` column from GWASCatalog

        Returns:
            Column: mapped using GWAS Catalog mapping

        Example:
            >>> import pyspark.sql.types as t
            >>> d = [("rs1234-A-G"), ("rs1234-A"), ("rs1234-A; rs1235-G")]
            >>> df = spark.createDataFrame(d, t.StringType())
            >>> df.withColumn('normalised', GWASCatalogCuratedAssociationsParser._extract_risk_allele(f.col('value'))).show()
            +------------------+----------+
            |             value|normalised|
            +------------------+----------+
            |        rs1234-A-G|         A|
            |          rs1234-A|         A|
            |rs1234-A; rs1235-G|         A|
            +------------------+----------+
            <BLANKLINE>
        """
        # GWAS Catalog to risk allele mapping
        return f.split(f.split(risk_allele, "; ").getItem(0), "-").getItem(1)

    @staticmethod
    def _collect_rsids(
        snp_id: Column, snp_id_current: Column, risk_allele: Column
    ) -> Column:
        """It takes three columns, and returns an array of distinct values from those columns.

        Args:
            snp_id (Column): The original snp id from the GWAS catalog.
            snp_id_current (Column): The current snp id field is just a number at the moment (stored as a string). Adding 'rs' prefix if looks good.
            risk_allele (Column): The risk allele for the SNP.

        Returns:
            Column: An array of distinct values.
        """
        # The current snp id field is just a number at the moment (stored as a string). Adding 'rs' prefix if looks good.
        snp_id_current = f.when(
            snp_id_current.rlike("^[0-9]*$"),
            f.format_string("rs%s", snp_id_current),
        )
        # Cleaning risk allele:
        risk_allele = f.split(risk_allele, "-").getItem(0)

        # Collecting all values:
        return f.array_distinct(f.array(snp_id, snp_id_current, risk_allele))

    @staticmethod
    def _map_variants_to_gnomad_variants(
        gwas_associations: DataFrame, variant_index: VariantIndex
    ) -> DataFrame:
        """Add variant metadata in associations.

        Args:
            gwas_associations (DataFrame): raw GWAS Catalog associations.
            variant_index (VariantIndex): GnomaAD variants dataset with allele frequencies.

        Returns:
            DataFrame: GWAS Catalog associations data including `variantId`, `referenceAllele`,
            `alternateAllele`, `chromosome`, `position` with variant metadata
        """
        # Subset of GWAS Catalog associations required for resolving variant IDs:
        gwas_associations_subset = gwas_associations.select(
            "rowId",
            f.col("CHR_ID").alias("chromosome"),
            # The positions from GWAS Catalog are from ensembl that causes discrepancy for indels:
            f.col("CHR_POS").cast(IntegerType()).alias("ensemblPosition"),
            # List of all SNPs associated with the variant
            GWASCatalogCuratedAssociationsParser._collect_rsids(
                f.split(f.col("SNPS"), "; ").getItem(0),
                f.col("SNP_ID_CURRENT"),
                f.split(f.col("STRONGEST SNP-RISK ALLELE"), "; ").getItem(0),
            ).alias("rsIdsGwasCatalog"),
            GWASCatalogCuratedAssociationsParser._extract_risk_allele(
                f.col("STRONGEST SNP-RISK ALLELE")
            ).alias("riskAllele"),
        )

        # Subset of variant annotation required for GWAS Catalog annotations:
        va_subset = variant_index.df.select(
            "variantId",
            "chromosome",
            # Calculate the position in Ensembl coordinates for indels:
            GWASCatalogCuratedAssociationsParser.convert_gnomad_position_to_ensembl(
                f.col("position"),
                f.col("referenceAllele"),
                f.col("alternateAllele"),
            ).alias("ensemblPosition"),
            # Keeping GnomAD position:
            "position",
            f.col("rsIds").alias("rsIdsGnomad"),
            "referenceAllele",
            "alternateAllele",
            "alleleFrequencies",
            variant_index.max_maf().alias("maxMaf"),
        ).join(
            gwas_associations_subset.select("chromosome", "ensemblPosition").distinct(),
            on=["chromosome", "ensemblPosition"],
            how="inner",
        )

        # Semi-resolved ids (still contains duplicates when conclusion was not possible to make
        # based on rsIds or allele concordance)
        filtered_associations = (
            gwas_associations_subset.join(
                va_subset,
                on=["chromosome", "ensemblPosition"],
                how="left",
            )
            .withColumn(
                "rsIdFilter",
                GWASCatalogCuratedAssociationsParser._flag_mappings_to_retain(
                    f.col("rowId"),
                    GWASCatalogCuratedAssociationsParser._compare_rsids(
                        f.col("rsIdsGnomad"), f.col("rsIdsGwasCatalog")
                    ),
                ),
            )
            .withColumn(
                "concordanceFilter",
                GWASCatalogCuratedAssociationsParser._flag_mappings_to_retain(
                    f.col("rowId"),
                    GWASCatalogCuratedAssociationsParser._check_concordance(
                        f.col("riskAllele"),
                        f.col("referenceAllele"),
                        f.col("alternateAllele"),
                    ),
                ),
            )
            .filter(
                # Filter out rows where GWAS Catalog rsId does not match with GnomAD rsId,
                # but there is corresponding variant for the same association
                f.col("rsIdFilter")
                # or filter out rows where GWAS Catalog alleles are not concordant with GnomAD alleles,
                # but there is corresponding variant for the same association
                | f.col("concordanceFilter")
            )
        )

        # Keep only highest maxMaf variant per rowId
        fully_mapped_associations = get_record_with_maximum_value(
            filtered_associations, grouping_col="rowId", sorting_col="maxMaf"
        ).select(
            "rowId",
            "variantId",
            "referenceAllele",
            "alternateAllele",
            "chromosome",
            "position",
        )

        return gwas_associations.join(fully_mapped_associations, on="rowId", how="left")

    @staticmethod
    def _compare_rsids(gnomad: Column, gwas: Column) -> Column:
        """If the intersection of the two arrays is greater than 0, return True, otherwise return False.

        Args:
            gnomad (Column): rsids from gnomad
            gwas (Column): rsids from the GWAS Catalog

        Returns:
            Column: A boolean column that is true if the GnomAD rsIDs can be found in the GWAS rsIDs.

        Examples:
            >>> d = [
            ...    (1, ["rs123", "rs523"], ["rs123"]),
            ...    (2, [], ["rs123"]),
            ...    (3, ["rs123", "rs523"], []),
            ...    (4, [], []),
            ... ]
            >>> df = spark.createDataFrame(d, ['associationId', 'gnomad', 'gwas'])
            >>> df.withColumn("rsid_matches", GWASCatalogCuratedAssociationsParser._compare_rsids(f.col("gnomad"),f.col('gwas'))).show()
            +-------------+--------------+-------+------------+
            |associationId|        gnomad|   gwas|rsid_matches|
            +-------------+--------------+-------+------------+
            |            1|[rs123, rs523]|[rs123]|        true|
            |            2|            []|[rs123]|       false|
            |            3|[rs123, rs523]|     []|       false|
            |            4|            []|     []|       false|
            +-------------+--------------+-------+------------+
            <BLANKLINE>

        """
        return f.when(f.size(f.array_intersect(gnomad, gwas)) > 0, True).otherwise(
            False
        )

    @staticmethod
    def _flag_mappings_to_retain(
        association_id: Column, filter_column: Column
    ) -> Column:
        """Flagging mappings to drop for each association.

        Some associations have multiple mappings. Some has matching rsId others don't. We only
        want to drop the non-matching mappings, when a matching is available for the given association.
        This logic can be generalised for other measures eg. allele concordance.

        Args:
            association_id (Column): association identifier column
            filter_column (Column): boolean col indicating to keep a mapping

        Returns:
            Column: A column with a boolean value.

        Examples:
        >>> d = [
        ...    (1, False),
        ...    (1, False),
        ...    (2, False),
        ...    (2, True),
        ...    (3, True),
        ...    (3, True),
        ... ]
        >>> df = spark.createDataFrame(d, ['associationId', 'filter'])
        >>> df.withColumn("isConcordant", GWASCatalogCuratedAssociationsParser._flag_mappings_to_retain(f.col("associationId"),f.col('filter'))).show()
        +-------------+------+------------+
        |associationId|filter|isConcordant|
        +-------------+------+------------+
        |            1| false|        true|
        |            1| false|        true|
        |            2| false|       false|
        |            2|  true|        true|
        |            3|  true|        true|
        |            3|  true|        true|
        +-------------+------+------------+
        <BLANKLINE>

        """
        w = Window.partitionBy(association_id)

        # Generating a boolean column informing if the filter column contains true anywhere for the association:
        aggregated_filter = f.when(
            f.array_contains(f.collect_set(filter_column).over(w), True), True
        ).otherwise(False)

        # Generate a filter column:
        return f.when(aggregated_filter & (~filter_column), False).otherwise(True)

    @staticmethod
    def _check_concordance(
        risk_allele: Column, reference_allele: Column, alternate_allele: Column
    ) -> Column:
        """A function to check if the risk allele is concordant with the alt or ref allele.

        If the risk allele is the same as the reference or alternate allele, or if the reverse complement of
        the risk allele is the same as the reference or alternate allele, then the allele is concordant.
        If no mapping is available (ref/alt is null), the function returns True.

        Args:
            risk_allele (Column): The allele that is associated with the risk of the disease.
            reference_allele (Column): The reference allele from the GWAS catalog
            alternate_allele (Column): The alternate allele of the variant.

        Returns:
            Column: A boolean column that is True if the risk allele is the same as the reference or alternate allele,
            or if the reverse complement of the risk allele is the same as the reference or alternate allele.

        Examples:
            >>> d = [
            ...     ('A', 'A', 'G'),
            ...     ('A', 'T', 'G'),
            ...     ('A', 'C', 'G'),
            ...     ('A', 'A', '?'),
            ...     (None, None, 'A'),
            ... ]
            >>> df = spark.createDataFrame(d, ['riskAllele', 'referenceAllele', 'alternateAllele'])
            >>> df.withColumn("isConcordant", GWASCatalogCuratedAssociationsParser._check_concordance(f.col("riskAllele"),f.col('referenceAllele'), f.col('alternateAllele'))).show()
            +----------+---------------+---------------+------------+
            |riskAllele|referenceAllele|alternateAllele|isConcordant|
            +----------+---------------+---------------+------------+
            |         A|              A|              G|        true|
            |         A|              T|              G|        true|
            |         A|              C|              G|       false|
            |         A|              A|              ?|        true|
            |      NULL|           NULL|              A|        true|
            +----------+---------------+---------------+------------+
            <BLANKLINE>

        """
        # Calculating the reverse complement of the risk allele:
        risk_allele_reverse_complement = f.when(
            risk_allele.rlike(r"^[ACTG]+$"),
            f.reverse(f.translate(risk_allele, "ACTG", "TGAC")),
        ).otherwise(risk_allele)

        # OK, is the risk allele or the reverse complent is the same as the mapped alleles:
        return (
            f.when(
                (risk_allele == reference_allele) | (risk_allele == alternate_allele),
                True,
            )
            # If risk allele is found on the negative strand:
            .when(
                (risk_allele_reverse_complement == reference_allele)
                | (risk_allele_reverse_complement == alternate_allele),
                True,
            )
            # If risk allele is ambiguous, still accepted: < This condition could be reconsidered
            .when(risk_allele == "?", True)
            # If the association could not be mapped we keep it:
            .when(reference_allele.isNull(), True)
            # Allele is discordant:
            .otherwise(False)
        )

    @staticmethod
    def _get_reverse_complement(allele_col: Column) -> Column:
        """A function to return the reverse complement of an allele column.

        It takes a string and returns the reverse complement of that string if it's a DNA sequence,
        otherwise it returns the original string. Assumes alleles in upper case.

        Args:
            allele_col (Column): The column containing the allele to reverse complement.

        Returns:
            Column: A column that is the reverse complement of the allele column.

        Examples:
            >>> d = [{"allele": 'A'}, {"allele": 'T'},{"allele": 'G'}, {"allele": 'C'},{"allele": 'AC'}, {"allele": 'GTaatc'},{"allele": '?'}, {"allele": None}]
            >>> df = spark.createDataFrame(d)
            >>> df.withColumn("revcom_allele", GWASCatalogCuratedAssociationsParser._get_reverse_complement(f.col("allele"))).show()
            +------+-------------+
            |allele|revcom_allele|
            +------+-------------+
            |     A|            T|
            |     T|            A|
            |     G|            C|
            |     C|            G|
            |    AC|           GT|
            |GTaatc|       GATTAC|
            |     ?|            ?|
            |  NULL|         NULL|
            +------+-------------+
            <BLANKLINE>

        """
        allele_col = f.upper(allele_col)
        return f.when(
            allele_col.rlike("[ACTG]+"),
            f.reverse(f.translate(allele_col, "ACTG", "TGAC")),
        ).otherwise(allele_col)

    @staticmethod
    def _effect_needs_harmonisation(
        risk_allele: Column, reference_allele: Column
    ) -> Column:
        """A function to check if the effect allele needs to be harmonised.

        Args:
            risk_allele (Column): Risk allele column
            reference_allele (Column): Effect allele column

        Returns:
            Column: A boolean column indicating if the effect allele needs to be harmonised.

        Examples:
            >>> d = [{"risk": 'A', "reference": 'A'}, {"risk": 'A', "reference": 'T'}, {"risk": 'AT', "reference": 'TA'}, {"risk": 'AT', "reference": 'AT'}]
            >>> df = spark.createDataFrame(d)
            >>> df.withColumn("needs_harmonisation", GWASCatalogCuratedAssociationsParser._effect_needs_harmonisation(f.col("risk"), f.col("reference"))).show()
            +---------+----+-------------------+
            |reference|risk|needs_harmonisation|
            +---------+----+-------------------+
            |        A|   A|               true|
            |        T|   A|               true|
            |       TA|  AT|              false|
            |       AT|  AT|               true|
            +---------+----+-------------------+
            <BLANKLINE>

        """
        return (risk_allele == reference_allele) | (
            risk_allele
            == GWASCatalogCuratedAssociationsParser._get_reverse_complement(
                reference_allele
            )
        )

    @staticmethod
    def _are_alleles_palindromic(
        reference_allele: Column, alternate_allele: Column
    ) -> Column:
        """A function to check if the alleles are palindromic.

        Args:
            reference_allele (Column): Reference allele column
            alternate_allele (Column): Alternate allele column

        Returns:
            Column: A boolean column indicating if the alleles are palindromic.

        Examples:
            >>> d = [{"reference": 'A', "alternate": 'T'}, {"reference": 'AT', "alternate": 'AG'}, {"reference": 'AT', "alternate": 'AT'}, {"reference": 'CATATG', "alternate": 'CATATG'}, {"reference": '-', "alternate": None}]
            >>> df = spark.createDataFrame(d)
            >>> df.withColumn("is_palindromic", GWASCatalogCuratedAssociationsParser._are_alleles_palindromic(f.col("reference"), f.col("alternate"))).show()
            +---------+---------+--------------+
            |alternate|reference|is_palindromic|
            +---------+---------+--------------+
            |        T|        A|          true|
            |       AG|       AT|         false|
            |       AT|       AT|          true|
            |   CATATG|   CATATG|          true|
            |     NULL|        -|         false|
            +---------+---------+--------------+
            <BLANKLINE>

        """
        revcomp = GWASCatalogCuratedAssociationsParser._get_reverse_complement(
            alternate_allele
        )
        return (
            f.when(reference_allele == revcomp, True)
            .when(revcomp.isNull(), False)
            .otherwise(False)
        )

    @staticmethod
    def _harmonise_beta(
        effect_size: Column,
        confidence_interval: Column,
        flipping_needed: Column,
    ) -> Column:
        """A function to extract the beta value from the effect size and confidence interval and harmonises for the alternate allele.

        If the confidence interval contains the word "increase" or "decrease" it indicates, we are dealing with betas.
        If it's "increase" and the effect size needs to be harmonized, then multiply the effect size by -1.
        The sign of the effect size is flipped if the confidence interval contains "decrease".

        eg. if the reported value is 0.5, and the confidence interval tells "decrease"? -> beta is -0.5

        Args:
            effect_size (Column): GWAS Catalog effect size column.
            confidence_interval (Column): GWAS Catalog confidence interval column to know the direction of the effect.
            flipping_needed (Column): Boolean flag indicating if effect needs to be flipped based on the alleles.

        Returns:
            Column: A column containing the beta value.

        Examples:
            >>> d = [
            ...    # positive effect -no flipping:
            ...    (0.5, 'increase', False),
            ...    # Positive effect - flip:
            ...    (0.5, 'decrease', False),
            ...    # Positive effect - flip:
            ...    (0.5, 'decrease', True),
            ...    # Negative effect - no flip:
            ...    (0.5, 'increase', True),
            ...    # Negative effect - flip:
            ...    (0.5, 'decrease', False),
            ... ]
            >>> (
            ...    spark.createDataFrame(d, ['effect', 'ci_text', 'flip'])
            ...    .select("effect", "ci_text", 'flip', GWASCatalogCuratedAssociationsParser._harmonise_beta(f.col("effect"), f.col("ci_text"), f.lit(False)).alias("beta"))
            ...    .show()
            ... )
            +------+--------+-----+----+
            |effect| ci_text| flip|beta|
            +------+--------+-----+----+
            |   0.5|increase|false| 0.5|
            |   0.5|decrease|false|-0.5|
            |   0.5|decrease| true|-0.5|
            |   0.5|increase| true| 0.5|
            |   0.5|decrease|false|-0.5|
            +------+--------+-----+----+
            <BLANKLINE>
        """
        return (
            f.when(
                (flipping_needed & confidence_interval.contains("increase"))
                | (~flipping_needed & confidence_interval.contains("decrease")),
                -effect_size,
            )
            .otherwise(effect_size)
            .cast(DoubleType())
        )

    @staticmethod
    def _harmonise_odds_ratio(
        effect_size: Column,
        flipping_needed: Column,
    ) -> Column:
        """Odds ratio is either propagated as is, or flipped if indicated, meaning returning a reciprocal value.

        Args:
            effect_size (Column): containing effect size,
            flipping_needed (Column): Boolean flag indicating if effect needs to be flipped

        Returns:
            Column: A column with the odds ratio, or 1/odds_ratio if harmonization required.

        Examples:
        >>> d = [(0.5, False), (0.5, True), (0.0, False), (0.0, True)]
        >>> (
        ...    spark.createDataFrame(d, ['effect', 'flip'])
        ...    .select("effect", "flip", GWASCatalogCuratedAssociationsParser._harmonise_odds_ratio(f.col("effect"), f.col("flip")).alias("odds_ratio"))
        ...    .show()
        ... )
        +------+-----+----------+
        |effect| flip|odds_ratio|
        +------+-----+----------+
        |   0.5|false|       0.5|
        |   0.5| true|       2.0|
        |   0.0|false|       0.0|
        |   0.0| true|      NULL|
        +------+-----+----------+
        <BLANKLINE>
        """
        return (
            # We are not flipping zero effect size:
            f.when((effect_size.cast(DoubleType()) == 0) & flipping_needed, f.lit(None))
            .when(
                flipping_needed,
                1 / effect_size,
            )
            .otherwise(effect_size)
            .cast(DoubleType())
        )

    @staticmethod
    def _concatenate_substudy_description(
        association_trait: Column, pvalue_text: Column, mapped_trait_uri: Column
    ) -> Column:
        """Substudy description parsing. Complex string containing metadata about the substudy (e.g. QTL, specific EFO, etc.).

        Args:
            association_trait (Column): GWAS Catalog association trait column
            pvalue_text (Column): GWAS Catalog p-value text column
            mapped_trait_uri (Column): GWAS Catalog mapped trait URI column

        Returns:
            Column: A column with the substudy description in the shape trait|pvaluetext1_pvaluetext2|EFO1_EFO2.

        Examples:
        >>> df = spark.createDataFrame([
        ...    ("Height", "http://www.ebi.ac.uk/efo/EFO_0000001,http://www.ebi.ac.uk/efo/EFO_0000002", "European Ancestry"),
        ...    ("Schizophrenia", "http://www.ebi.ac.uk/efo/MONDO_0005090", None)],
        ...    ["association_trait", "mapped_trait_uri", "pvalue_text"]
        ... )
        >>> df.withColumn('substudy_description', GWASCatalogCuratedAssociationsParser._concatenate_substudy_description(df.association_trait, df.pvalue_text, df.mapped_trait_uri)).show(truncate=False)
        +-----------------+-------------------------------------------------------------------------+-----------------+------------------------------------------+
        |association_trait|mapped_trait_uri                                                         |pvalue_text      |substudy_description                      |
        +-----------------+-------------------------------------------------------------------------+-----------------+------------------------------------------+
        |Height           |http://www.ebi.ac.uk/efo/EFO_0000001,http://www.ebi.ac.uk/efo/EFO_0000002|European Ancestry|Height|EA|EFO_0000001/EFO_0000002         |
        |Schizophrenia    |http://www.ebi.ac.uk/efo/MONDO_0005090                                   |NULL             |Schizophrenia|no_pvalue_text|MONDO_0005090|
        +-----------------+-------------------------------------------------------------------------+-----------------+------------------------------------------+
        <BLANKLINE>
        """
        p_value_text = f.coalesce(
            GWASCatalogCuratedAssociationsParser._normalise_pvaluetext(pvalue_text),
            f.array(f.lit("no_pvalue_text")),
        )
        return f.concat_ws(
            "|",
            association_trait,
            f.concat_ws(
                "/",
                p_value_text,
            ),
            f.concat_ws(
                "/",
                parse_efos(mapped_trait_uri),
            ),
        )

    @staticmethod
    def _qc_all(
        qc: Column,
        chromosome: Column,
        position: Column,
        reference_allele: Column,
        alternate_allele: Column,
        strongest_snp_risk_allele: Column,
        p_value_mantissa: Column,
        p_value_exponent: Column,
        p_value_cutoff: float,
    ) -> Column:
        """Flag associations that fail any QC.

        Args:
            qc (Column): QC column
            chromosome (Column): Chromosome column
            position (Column): Position column
            reference_allele (Column): Reference allele column
            alternate_allele (Column): Alternate allele column
            strongest_snp_risk_allele (Column): Strongest SNP risk allele column
            p_value_mantissa (Column): P-value mantissa column
            p_value_exponent (Column): P-value exponent column
            p_value_cutoff (float): P-value cutoff

        Returns:
            Column: Updated QC column with flag.
        """
        qc = GWASCatalogCuratedAssociationsParser._qc_variant_interactions(
            qc, strongest_snp_risk_allele
        )
        qc = StudyLocus._qc_subsignificant_associations(
            qc, p_value_mantissa, p_value_exponent, p_value_cutoff
        )
        qc = GWASCatalogCuratedAssociationsParser._qc_genomic_location(
            qc, chromosome, position
        )
        qc = GWASCatalogCuratedAssociationsParser._qc_variant_inconsistencies(
            qc, chromosome, position, strongest_snp_risk_allele
        )
        qc = GWASCatalogCuratedAssociationsParser._qc_unmapped_variants(
            qc, alternate_allele
        )
        qc = GWASCatalogCuratedAssociationsParser._qc_palindromic_alleles(
            qc, reference_allele, alternate_allele
        )
        return qc

    @staticmethod
    def _qc_variant_interactions(
        qc: Column, strongest_snp_risk_allele: Column
    ) -> Column:
        """Flag associations based on variant x variant interactions.

        Args:
            qc (Column): QC column
            strongest_snp_risk_allele (Column): Column with the strongest SNP risk allele

        Returns:
            Column: Updated QC column with flag.
        """
        return StudyLocus.update_quality_flag(
            qc,
            strongest_snp_risk_allele.contains(";"),
            StudyLocusQualityCheck.COMPOSITE_FLAG,
        )

    @staticmethod
    def _qc_genomic_location(
        qc: Column, chromosome: Column, position: Column
    ) -> Column:
        """Flag associations without genomic location in GWAS Catalog.

        Args:
            qc (Column): QC column
            chromosome (Column): Chromosome column in GWAS Catalog
            position (Column): Position column in GWAS Catalog

        Returns:
            Column: Updated QC column with flag.

        Examples:
            >>> import pyspark.sql.types as t
            >>> d = [{'qc': None, 'chromosome': None, 'position': None}, {'qc': None, 'chromosome': '1', 'position': None}, {'qc': None, 'chromosome': None, 'position': 1}, {'qc': None, 'chromosome': '1', 'position': 1}]
            >>> df = spark.createDataFrame(d, schema=t.StructType([t.StructField('qc', t.ArrayType(t.StringType()), True), t.StructField('chromosome', t.StringType()), t.StructField('position', t.IntegerType())]))
            >>> df.withColumn('qc', GWASCatalogCuratedAssociationsParser._qc_genomic_location(df.qc, df.chromosome, df.position)).show(truncate=False)
            +----------------------------+----------+--------+
            |qc                          |chromosome|position|
            +----------------------------+----------+--------+
            |[Incomplete genomic mapping]|NULL      |NULL    |
            |[Incomplete genomic mapping]|1         |NULL    |
            |[Incomplete genomic mapping]|NULL      |1       |
            |[]                          |1         |1       |
            +----------------------------+----------+--------+
            <BLANKLINE>

        """
        return StudyLocus.update_quality_flag(
            qc,
            position.isNull() | chromosome.isNull(),
            StudyLocusQualityCheck.NO_GENOMIC_LOCATION_FLAG,
        )

    @staticmethod
    def _qc_variant_inconsistencies(
        qc: Column,
        chromosome: Column,
        position: Column,
        strongest_snp_risk_allele: Column,
    ) -> Column:
        """Flag associations with inconsistencies in the variant annotation.

        Args:
            qc (Column): QC column
            chromosome (Column): Chromosome column in GWAS Catalog
            position (Column): Position column in GWAS Catalog
            strongest_snp_risk_allele (Column): Strongest SNP risk allele column in GWAS Catalog

        Returns:
            Column: Updated QC column with flag.
        """
        return StudyLocus.update_quality_flag(
            qc,
            # Number of chromosomes does not correspond to the number of positions:
            (f.size(f.split(chromosome, ";")) != f.size(f.split(position, ";")))
            # Number of chromosome values different from riskAllele values:
            | (
                f.size(f.split(chromosome, ";"))
                != f.size(f.split(strongest_snp_risk_allele, ";"))
            ),
            StudyLocusQualityCheck.INCONSISTENCY_FLAG,
        )

    @staticmethod
    def _qc_unmapped_variants(qc: Column, alternate_allele: Column) -> Column:
        """Flag associations with variants not mapped to variantAnnotation.

        Args:
            qc (Column): QC column
            alternate_allele (Column): alternate allele

        Returns:
            Column: Updated QC column with flag.

        Example:
            >>> import pyspark.sql.types as t
            >>> d = [{'alternate_allele': 'A', 'qc': None}, {'alternate_allele': None, 'qc': None}]
            >>> schema = t.StructType([t.StructField('alternate_allele', t.StringType(), True), t.StructField('qc', t.ArrayType(t.StringType()), True)])
            >>> df = spark.createDataFrame(data=d, schema=schema)
            >>> df.withColumn("new_qc", GWASCatalogCuratedAssociationsParser._qc_unmapped_variants(f.col("qc"), f.col("alternate_allele"))).show()
            +----------------+----+--------------------+
            |alternate_allele|  qc|              new_qc|
            +----------------+----+--------------------+
            |               A|NULL|                  []|
            |            NULL|NULL|[No mapping in Gn...|
            +----------------+----+--------------------+
            <BLANKLINE>

        """
        return StudyLocus.update_quality_flag(
            qc,
            alternate_allele.isNull(),
            StudyLocusQualityCheck.NON_MAPPED_VARIANT_FLAG,
        )

    @staticmethod
    def _qc_palindromic_alleles(
        qc: Column, reference_allele: Column, alternate_allele: Column
    ) -> Column:
        """Flag associations with palindromic variants which effects can not be harmonised.

        Args:
            qc (Column): QC column
            reference_allele (Column): reference allele
            alternate_allele (Column): alternate allele

        Returns:
            Column: Updated QC column with flag.

        Example:
            >>> import pyspark.sql.types as t
            >>> schema = t.StructType([t.StructField('reference_allele', t.StringType(), True), t.StructField('alternate_allele', t.StringType(), True), t.StructField('qc', t.ArrayType(t.StringType()), True)])
            >>> d = [{'reference_allele': 'A', 'alternate_allele': 'T', 'qc': None}, {'reference_allele': 'AT', 'alternate_allele': 'TA', 'qc': None}, {'reference_allele': 'AT', 'alternate_allele': 'AT', 'qc': None}]
            >>> df = spark.createDataFrame(data=d, schema=schema)
            >>> df.withColumn("qc", GWASCatalogCuratedAssociationsParser._qc_palindromic_alleles(f.col("qc"), f.col("reference_allele"), f.col("alternate_allele"))).show(truncate=False)
            +----------------+----------------+---------------------------------------+
            |reference_allele|alternate_allele|qc                                     |
            +----------------+----------------+---------------------------------------+
            |A               |T               |[Palindrome alleles - cannot harmonize]|
            |AT              |TA              |[]                                     |
            |AT              |AT              |[Palindrome alleles - cannot harmonize]|
            +----------------+----------------+---------------------------------------+
            <BLANKLINE>

        """
        return StudyLocus.update_quality_flag(
            qc,
            GWASCatalogCuratedAssociationsParser._are_alleles_palindromic(
                reference_allele, alternate_allele
            ),
            StudyLocusQualityCheck.PALINDROMIC_ALLELE_FLAG,
        )

    @staticmethod
    def _get_effect_type(ci_text: Column) -> Column:
        """Extracts the effect type from the 95% CI text.

        The GWAS Catalog confidence interval column contains text that can be used to infer the effect type.
        If the text contains "increase" or "decrease", the effect type is beta, otherwise it is odds ratio.
        Null columns return null as the effect type.

        Args:
            ci_text (Column): Column containing the 95% CI text.

        Returns:
            Column: A column containing the effect type.

        Examples:
            >>> data = [{"ci_text": "95% CI: [0.1-0.2]"}, {"ci_text": "95% CI: [0.1-0.2] increase"}, {"ci_text": "95% CI: [0.1-0.2] decrease"}, {"ci_text": None}]
            >>> spark.createDataFrame(data).select('ci_text', GWASCatalogCuratedAssociationsParser._get_effect_type(f.col('ci_text')).alias('effect_type')).show(truncate=False)
            +--------------------------+-----------+
            |ci_text                   |effect_type|
            +--------------------------+-----------+
            |95% CI: [0.1-0.2]         |odds_ratio |
            |95% CI: [0.1-0.2] increase|beta       |
            |95% CI: [0.1-0.2] decrease|beta       |
            |NULL                      |NULL       |
            +--------------------------+-----------+
            <BLANKLINE>

        """
        return f.when(
            f.lower(ci_text).contains("increase")
            | f.lower(ci_text).contains("decrease"),
            f.lit("beta"),
        ).when(ci_text.isNotNull(), f.lit("odds_ratio"))

    @staticmethod
    def harmonise_association_effect_to_beta(
        df: DataFrame,
    ) -> DataFrame:
        """Harmonise effect to beta value.

        The harmonisation process has a number of steps:
        - Extracting the reported effect allele.
        - Flagging palindromic alleles.
        - Flagging associations where the effect direction needs to be flipped.
        - Flagging the effect type.
        - Getting the standard error from the beta and neglog p-value or odds ratio confidence intervals.
        - Harmonising both beta and odds ratio.
        - Converting the odds ratio to beta.

        Args:
            df (DataFrame): DataFrame with the following columns:

        Returns:
            DataFrame: DataFrame with the following columns:

        Raises:
            ValueError: If any of the required columns are missing.

        Examples:
        ---
        >>> data = [
        ...    # Flagged as palindromic:
        ...    ('rs123-T', 'A', 'T', '0.1', '[0.08-0.12] unit increase', 21.96),
        ...    # Not palindromic, beta needs to be flipped:
        ...    ('rs123-C', 'G', 'T', '0.1', '[0.08-0.12] unit increase', 21.96),
        ...    # Beta is not flipped:
        ...    ('rs123-T', 'C', 'T', '0.1', '[0.08-0.12] unit increase', 21.96),
        ...    # odds ratio:
        ...    ('rs123-T', 'C', 'T', '0.1', '[0.08-0.12]', 21.96),
        ...    # odds ratio flipped:
        ...    ('rs123-C', 'G', 'T', '0.1', '[0.08-0.12]', 21.96),
        ... ]
        >>> schema = ["STRONGEST SNP-RISK ALLELE", "referenceAllele", "alternateAllele", "OR or BETA", "95% CI (TEXT)", "PVALUE_MLOG"]
        >>> df = spark.createDataFrame(data, schema)
        >>> GWASCatalogCuratedAssociationsParser.harmonise_association_effect_to_beta(df).show()
        +-------------------------+---------------+---------------+----------+--------------------+-----------+-------------------+-------------------+
        |STRONGEST SNP-RISK ALLELE|referenceAllele|alternateAllele|OR or BETA|       95% CI (TEXT)|PVALUE_MLOG|               beta|      standardError|
        +-------------------------+---------------+---------------+----------+--------------------+-----------+-------------------+-------------------+
        |                  rs123-T|              A|              T|       0.1|[0.08-0.12] unit ...|      21.96|               NULL|               NULL|
        |                  rs123-C|              G|              T|       0.1|[0.08-0.12] unit ...|      21.96|               -0.1|0.01020130187396028|
        |                  rs123-T|              C|              T|       0.1|[0.08-0.12] unit ...|      21.96|                0.1|0.01020130187396028|
        |                  rs123-T|              C|              T|       0.1|         [0.08-0.12]|      21.96|-2.3025850929940455|0.23489365624113162|
        |                  rs123-C|              G|              T|       0.1|         [0.08-0.12]|      21.96|  2.302585092994046|0.23489365624113168|
        +-------------------------+---------------+---------------+----------+--------------------+-----------+-------------------+-------------------+
        <BLANKLINE>
        """
        # Testing if all columns are in the dataframe:
        required_columns = [
            "STRONGEST SNP-RISK ALLELE",
            "referenceAllele",
            "alternateAllele",
            "OR or BETA",
            "95% CI (TEXT)",
            "PVALUE_MLOG",
        ]

        for column in required_columns:
            if column not in df.columns:
                raise ValueError(
                    f"Column {column} is required for harmonising effect to beta value."
                )

        pval_components = pvalue_from_neglogpval(f.col("PVALUE_MLOG"))

        return (
            df.withColumn(
                "reportedRiskAllele",
                GWASCatalogCuratedAssociationsParser._extract_risk_allele(
                    f.col("STRONGEST SNP-RISK ALLELE")
                ),
            )
            .withColumns(
                {
                    # Flag palindromic alleles:
                    "isAllelePalindromic": GWASCatalogCuratedAssociationsParser._are_alleles_palindromic(
                        f.col("referenceAllele"), f.col("alternateAllele")
                    ),
                    # Flag associations, where the effect direction needs to be flipped:
                    "needsFlipping": GWASCatalogCuratedAssociationsParser._effect_needs_harmonisation(
                        f.col("reportedRiskAllele"), f.col("referenceAllele")
                    ),
                    # Flag effect type:
                    "effectType": GWASCatalogCuratedAssociationsParser._get_effect_type(
                        f.col("95% CI (TEXT)")
                    ),
                }
            )
            # Harmonise both beta and odds ratio:
            .withColumns(
                {  # Normalise beta value of the association:
                    "effect_beta": f.when(
                        (f.col("effectType") == "beta")
                        & (~f.col("isAllelePalindromic")),
                        GWASCatalogCuratedAssociationsParser._harmonise_beta(
                            f.col("OR or BETA"),
                            f.col("95% CI (TEXT)"),
                            f.col("needsFlipping"),
                        ),
                    ),
                    # Normalise odds ratio of the association:
                    "effect_odds_ratio": f.when(
                        (f.col("effectType") == "odds_ratio")
                        & (~f.col("isAllelePalindromic")),
                        GWASCatalogCuratedAssociationsParser._harmonise_odds_ratio(
                            f.col("OR or BETA"),
                            f.col("needsFlipping"),
                        ),
                    ),
                },
            )
            .select(
                *df.columns,
                # Harmonise beta and standard error:
                *normalise_gwas_statistics(
                    beta=f.col("effect_beta"),
                    odds_ratio=f.col("effect_odds_ratio"),
                    standard_error=f.lit(None).alias("standardError"),
                    ci_lower=f.regexp_extract(
                        "95% CI (TEXT)", r"\[(\d+\.*\d*)-\d+\.*\d*\]", 1
                    ).cast(FloatType()),
                    ci_upper=f.regexp_extract(
                        "95% CI (TEXT)", r"\[\d+\.*\d*-(\d+\.*\d*)\]", 1
                    ).cast(FloatType()),
                    mantissa=pval_components.mantissa,
                    exponent=pval_components.exponent,
                ),
            )
        )

    @classmethod
    def from_source(
        cls: type[GWASCatalogCuratedAssociationsParser],
        gwas_associations: DataFrame,
        gnomad_variants: VariantIndex,
        pvalue_threshold: float = WindowBasedClumpingStepConfig.gwas_significance,
    ) -> StudyLocusGWASCatalog:
        """Read GWASCatalog associations.

        It reads the GWAS Catalog association dataset, selects and renames columns, casts columns, and
        applies some pre-defined filters on the data:

        Args:
            gwas_associations (DataFrame): GWAS Catalog raw associations dataset.
            gnomad_variants (VariantIndex): Variant dataset from GnomAD, with allele frequencies.
            pvalue_threshold (float): P-value threshold for flagging associations.

        Returns:
            StudyLocusGWASCatalog: GWASCatalogAssociations dataset

        pvalue_threshold is kept in sync with the WindowBasedClumpingStep gwas_significance.
        """
        pvalue_columns = GWASCatalogCuratedAssociationsParser._split_pvalue_column(
            f.col("P-VALUE")
        )
        return StudyLocusGWASCatalog(
            _df=gwas_associations.withColumn(
                # temporary column
                "rowId",
                f.monotonically_increasing_id().cast(StringType()),
            )
            .transform(
                # Map/harmonise variants to variant annotation dataset:
                # This function adds columns: variantId, referenceAllele, alternateAllele, chromosome, position
                lambda df: GWASCatalogCuratedAssociationsParser._map_variants_to_gnomad_variants(
                    df, gnomad_variants
                )
            )
            .withColumns(
                # Perform all quality control checks:
                {
                    "qualityControls": GWASCatalogCuratedAssociationsParser._qc_all(
                        qc=f.array().alias("qualityControls"),
                        chromosome=f.col("CHR_ID"),
                        position=f.col("CHR_POS").cast(IntegerType()),
                        reference_allele=f.col("referenceAllele"),
                        alternate_allele=f.col("alternateAllele"),
                        strongest_snp_risk_allele=f.col("STRONGEST SNP-RISK ALLELE"),
                        p_value_mantissa=pvalue_columns.mantissa,
                        p_value_exponent=pvalue_columns.exponent,
                        p_value_cutoff=pvalue_threshold,
                    )
                }
            )
            # Harmonising effect to beta value and flip effect if needed:
            .withColumns(
                {
                    "pValueMantissa": pvalue_columns.mantissa,
                    "pValueExponent": pvalue_columns.exponent,
                }
            )
            .transform(cls.harmonise_association_effect_to_beta)
            .withColumnRenamed("STUDY ACCESSION", "studyId")
            # Adding study-locus id:
            .withColumn(
                "studyLocusId",
                StudyLocus.assign_study_locus_id(["studyId", "variantId"]),
            )
            .select(
                # INSIDE STUDY-LOCUS SCHEMA:
                "studyLocusId",
                "variantId",
                # Mapped genomic location of the variant (; separated list)
                "chromosome",
                "position",
                "studyId",
                # p-value of the association, string: split into exponent and mantissa.
                "pValueMantissa",
                "pValueExponent",
                # Capturing phenotype granularity at the association level
                GWASCatalogCuratedAssociationsParser._concatenate_substudy_description(
                    f.col("DISEASE/TRAIT"),
                    f.col("P-VALUE (TEXT)"),
                    f.col("MAPPED_TRAIT_URI"),
                ).alias("subStudyDescription"),
                # Quality controls (array of strings)
                "qualityControls",
                "beta",
                "standardError",
            ),
            _schema=StudyLocusGWASCatalog.get_schema(),
        )


@dataclass
class StudyLocusGWASCatalog(StudyLocus):
    """Study locus Dataset for GWAS Catalog curated associations.

    A study index dataset captures all the metadata for all studies including GWAS and Molecular QTL.
    """

    def update_study_id(
        self: StudyLocusGWASCatalog, study_annotation: DataFrame
    ) -> StudyLocusGWASCatalog:
        """Update final studyId and studyLocusId with a dataframe containing study annotation.

        Args:
            study_annotation (DataFrame): Dataframe containing `updatedStudyId` and key columns `studyId` and `subStudyDescription`.

        Returns:
            StudyLocusGWASCatalog: Updated study locus with new `studyId` and `studyLocusId`.
        """
        self.df = (
            self._df.join(
                study_annotation, on=["studyId", "subStudyDescription"], how="left"
            )
            .withColumn("studyId", f.coalesce("updatedStudyId", "studyId"))
            .drop("subStudyDescription", "updatedStudyId")
        ).withColumn(
            "studyLocusId",
            StudyLocus.assign_study_locus_id(["studyId", "variantId"]),
        )
        return self

    def qc_ambiguous_study(self: StudyLocusGWASCatalog) -> StudyLocusGWASCatalog:
        """Flag associations with variants that can not be unambiguously associated with one study.

        Returns:
            StudyLocusGWASCatalog: Updated study locus.
        """
        assoc_ambiguity_window = Window.partitionBy(
            f.col("studyId"), f.col("variantId")
        )

        self._df.withColumn(
            "qualityControls",
            StudyLocus.update_quality_flag(
                f.col("qualityControls"),
                f.count(f.col("variantId")).over(assoc_ambiguity_window) > 1,
                StudyLocusQualityCheck.AMBIGUOUS_STUDY,
            ),
        )
        return self

    def qc_flag_all_tophits(self: StudyLocusGWASCatalog) -> StudyLocusGWASCatalog:
        """Flag all associations as top hits.

        Returns:
            StudyLocusGWASCatalog: Updated study locus.
        """
        return StudyLocusGWASCatalog(
            _df=self._df.withColumn(
                "qualityControls",
                StudyLocus.update_quality_flag(
                    f.col("qualityControls"),
                    f.lit(True),
                    StudyLocusQualityCheck.TOP_HIT,
                ),
            ),
            _schema=StudyLocusGWASCatalog.get_schema(),
        )

    def apply_inclusion_list(
        self: StudyLocusGWASCatalog, inclusion_list: DataFrame
    ) -> StudyLocusGWASCatalog:
        """Restricting GWAS Catalog studies based on a list of accpected study ids.

        Args:
            inclusion_list (DataFrame): List of accepted GWAS Catalog study identifiers

        Returns:
            StudyLocusGWASCatalog: Filtered dataset.
        """
        return StudyLocusGWASCatalog(
            _df=self.df.join(inclusion_list, on="studyId", how="inner"),
            _schema=StudyLocusGWASCatalog.get_schema(),
        )
