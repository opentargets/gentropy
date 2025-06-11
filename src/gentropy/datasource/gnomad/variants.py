"""Import gnomAD variants dataset."""

from __future__ import annotations

import hail as hl
import pyspark.sql.functions as f
import pyspark.sql.types as t

from gentropy.common.types import VariantPopulation
from gentropy.config import GnomadVariantConfig, VariantIndexConfig
from gentropy.dataset.variant_index import VariantIndex


class GnomADVariantFrequencies:
    """Extract GnomAD variants including allele frequencies."""

    def __init__(
        self: GnomADVariantFrequencies,
        gnomad_joint_path: str = GnomadVariantConfig().gnomad_joint_path,
        gnomad_variant_populations: list[
            VariantPopulation | str
        ] = GnomadVariantConfig().gnomad_variant_populations,
        hash_threshold: int = VariantIndexConfig().hash_threshold,
    ):
        """Initialize.

        Args:
            gnomad_joint_path (str): Path to gnomAD "joint" hail table.
            gnomad_variant_populations (list[VariantPopulation | str]): List of populations to include.
            hash_threshold (int): longer variant ids will be hashed.

        All defaults are stored in GnomadVariantConfig.
        """
        self.gnomad_joint_path = gnomad_joint_path
        self.gnomad_variant_populations = gnomad_variant_populations
        self.lenght_threshold = hash_threshold

    def as_variant_index(self: GnomADVariantFrequencies) -> VariantIndex:
        """Generate variant annotation dataset from gnomAD.

        Some relevant modifications to the original dataset are:

        1. The transcript consequences features provided by VEP are filtered to only refer to the Ensembl canonical transcript.
        2. Genome coordinates are liftovered from GRCh38 to GRCh37 to keep as annotation.
        3. Field names are converted to camel case to follow the convention.

        Returns:
            VariantIndex: GnomaAD variants dataset.
        """
        # Load variants dataset
        ht = hl.read_table(
            self.gnomad_joint_path,
            _load_refs=False,
        )

        # Drop non biallelic variants
        ht = ht.filter(ht.alleles.length() == 2)

        # Select relevant fields and nested records to create class
        return VariantIndex(
            _df=(
                ht.select(
                    # Extract mandatory fields:
                    variantId=hl.str("_").join(
                        [
                            ht.locus.contig.replace("chr", ""),
                            hl.str(ht.locus.position),
                            ht.alleles[0],
                            ht.alleles[1],
                        ]
                    ),
                    chromosome=ht.locus.contig.replace("chr", ""),
                    position=ht.locus.position,
                    referenceAllele=ht.alleles[0],
                    alternateAllele=ht.alleles[1],
                    # Extract allele frequencies from populations of interest:
                    alleleFrequencies=hl.set(
                        [f"{pop}_adj" for pop in self.gnomad_variant_populations]
                    ).map(
                        lambda p: hl.struct(
                            populationName=p,
                            alleleFrequency=ht.joint.freq[
                                ht.joint_globals.freq_index_dict[p]
                            ].AF,
                        )
                    ),
                    # Extract cross references to GnomAD:
                    dbXrefs=hl.array(
                        [
                            hl.struct(
                                id=hl.str("-").join(
                                    [
                                        ht.locus.contig.replace("chr", ""),
                                        hl.str(ht.locus.position),
                                        ht.alleles[0],
                                        ht.alleles[1],
                                    ]
                                ),
                                source=hl.str("gnomad"),
                            )
                        ]
                    ),
                )
                .key_by("chromosome", "position")
                .drop("locus", "alleles")
                .select_globals()
                .to_spark(flatten=False)
                .withColumns(
                    {
                        # Generate a variantId that is hashed for long variant ids:
                        "variantId": VariantIndex.hash_long_variant_ids(
                            f.col("variantId"),
                            f.col("chromosome"),
                            f.col("position"),
                            self.lenght_threshold,
                        ),
                        # We are not capturing the most severe consequence from GnomAD, but this column needed for the schema:
                        "mostSevereConsequenceId": f.lit(None).cast(t.StringType()),
                    }
                )
            ),
            _schema=VariantIndex.get_schema(),
        )


class GnomADVariantRsIds:
    """Extract GnomAD variants including variant Rs identifiers."""

    def __init__(
        self: GnomADVariantRsIds,
        gnomad_genomes_path: str = GnomadVariantConfig().gnomad_genomes_path,
        hash_threshold: int = VariantIndexConfig().hash_threshold,
    ):
        """Initialize.

        Args:
            gnomad_genomes_path (str): Path to gnomAD genomes hail table.
            hash_threshold (int): longer variant ids will be hashed.

        All defaults are stored in GnomadVariantConfig.
        """
        self.gnomad_genomes_path = gnomad_genomes_path
        self.lenght_threshold = hash_threshold

    def as_variant_index(self: GnomADVariantRsIds) -> VariantIndex:
        """Generate variant annotation dataset from gnomAD.

        Some relevant modifications to the original dataset are:

        1. The transcript consequences features provided by VEP are filtered to only refer to the Ensembl canonical transcript.
        2. Genome coordinates are liftovered from GRCh38 to GRCh37 to keep as annotation.
        3. Field names are converted to camel case to follow the convention.

        Returns:
            VariantIndex: GnomaAD variants dataset.
        """
        # Load variants dataset
        ht = hl.read_table(
            self.gnomad_genomes_path,
            _load_refs=False,
        )

        # Drop non biallelic variants
        ht = ht.filter(ht.alleles.length() == 2)

        # Select relevant fields and nested records to create class
        return VariantIndex(
            _df=(
                ht.select(
                    # Extract mandatory fields:
                    variantId=hl.str("_").join(
                        [
                            ht.locus.contig.replace("chr", ""),
                            hl.str(ht.locus.position),
                            ht.alleles[0],
                            ht.alleles[1],
                        ]
                    ),
                    chromosome=ht.locus.contig.replace("chr", ""),
                    position=ht.locus.position,
                    referenceAllele=ht.alleles[0],
                    alternateAllele=ht.alleles[1],
                    # Extract rsIds:
                    rsIds=ht.rsid,
                    # Extract cross references to GnomAD:
                    dbXrefs=hl.array(
                        [
                            hl.struct(
                                id=hl.str("-").join(
                                    [
                                        ht.locus.contig.replace("chr", ""),
                                        hl.str(ht.locus.position),
                                        ht.alleles[0],
                                        ht.alleles[1],
                                    ]
                                ),
                                source=hl.str("gnomad"),
                            )
                        ]
                    ),
                )
                .key_by("chromosome", "position")
                .drop("locus", "alleles")
                .select_globals()
                .to_spark(flatten=False)
                .withColumns(
                    {
                        # Generate a variantId that is hashed for long variant ids:
                        "variantId": VariantIndex.hash_long_variant_ids(
                            f.col("variantId"),
                            f.col("chromosome"),
                            f.col("position"),
                            self.lenght_threshold,
                        ),
                        # We are not capturing the most severe consequence from GnomAD, but this column needed for the schema:
                        "mostSevereConsequenceId": f.lit(None).cast(t.StringType()),
                    }
                )
            ),
            _schema=VariantIndex.get_schema(),
        )
