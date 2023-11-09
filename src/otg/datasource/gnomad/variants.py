"""Import gnomAD variants dataset."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import hail as hl

from otg.dataset.variant_annotation import VariantAnnotation

if TYPE_CHECKING:
    from hail.expr.expressions import Int32Expression, StringExpression


@dataclass
class GnomADVariants:
    """GnomAD variants included in the GnomAD genomes dataset.

    Attributes:
        gnomad_genomes (str): Path to gnomAD genomes hail table. Defaults to gnomAD's 3.1.2 release.
        chain_hail_38_37 (str): Path to GRCh38 to GRCh37 chain file. Defaults to Hail's chain file.
        populations (list[str]): List of populations to include. Defaults to all populations.
    """

    gnomad_genomes: str = "gs://gcp-public-data--gnomad/release/3.1.2/ht/genomes/gnomad.genomes.v3.1.2.sites.ht"
    chain_hail_38_37: str = "gs://hail-common/references/grch38_to_grch37.over.chain.gz"
    populations: list[str] = field(
        default_factory=lambda: [
            "afr",  # African-American
            "amr",  # American Admixed/Latino
            "ami",  # Amish ancestry
            "asj",  # Ashkenazi Jewish
            "eas",  # East Asian
            "fin",  # Finnish
            "nfe",  # Non-Finnish European
            "mid",  # Middle Eastern
            "sas",  # South Asian
            "oth",  # Other
        ]
    )

    @staticmethod
    def _convert_gnomad_position_to_ensembl_hail(
        position: Int32Expression,
        reference: StringExpression,
        alternate: StringExpression,
    ) -> Int32Expression:
        """Convert GnomAD variant position to Ensembl variant position in hail table.

        For indels (the reference or alternate allele is longer than 1), then adding 1 to the position, for SNPs, the position is unchanged.
        More info about the problem: https://www.biostars.org/p/84686/

        Args:
            position (Int32Expression): Position of the variant in the GnomAD genome.
            reference (StringExpression): The reference allele.
            alternate (StringExpression): The alternate allele

        Returns:
            Int32Expression: The position of the variant according to Ensembl genome.
        """
        return hl.if_else(
            (reference.length() > 1) | (alternate.length() > 1), position + 1, position
        )

    def as_variant_annotation(self: GnomADVariants) -> VariantAnnotation:
        """Generate variant annotation dataset from gnomAD.

        Some relevant modifications to the original dataset are:

        1. The transcript consequences features provided by VEP are filtered to only refer to the Ensembl canonical transcript.
        2. Genome coordinates are liftovered from GRCh38 to GRCh37 to keep as annotation.
        3. Field names are converted to camel case to follow the convention.

        Returns:
            VariantAnnotation: Variant annotation dataset
        """
        # Load variants dataset
        ht = hl.read_table(
            self.gnomad_genomes,
            _load_refs=False,
        )

        # Liftover
        grch37 = hl.get_reference("GRCh37")
        grch38 = hl.get_reference("GRCh38")
        grch38.add_liftover(self.chain_hail_38_37, grch37)

        # Drop non biallelic variants
        ht = ht.filter(ht.alleles.length() == 2)
        # Liftover
        ht = ht.annotate(locus_GRCh37=hl.liftover(ht.locus, "GRCh37"))
        # Select relevant fields and nested records to create class
        return VariantAnnotation(
            _df=(
                ht.head(30)
                .select(
                    gnomad3VariantId=hl.str("-").join(
                        [
                            ht.locus.contig.replace("chr", ""),
                            hl.str(ht.locus.position),
                            ht.alleles[0],
                            ht.alleles[1],
                        ]
                    ),
                    chromosome=ht.locus.contig.replace("chr", ""),
                    position=GnomADVariants._convert_gnomad_position_to_ensembl_hail(
                        ht.locus.position, ht.alleles[0], ht.alleles[1]
                    ),
                    variantId=hl.str("_").join(
                        [
                            ht.locus.contig.replace("chr", ""),
                            hl.str(
                                GnomADVariants._convert_gnomad_position_to_ensembl_hail(
                                    ht.locus.position, ht.alleles[0], ht.alleles[1]
                                )
                            ),
                            ht.alleles[0],
                            ht.alleles[1],
                        ]
                    ),
                    chromosomeB37=ht.locus_GRCh37.contig.replace("chr", ""),
                    positionB37=ht.locus_GRCh37.position,
                    referenceAllele=ht.alleles[0],
                    alternateAllele=ht.alleles[1],
                    rsIds=ht.rsid,
                    alleleType=ht.allele_info.allele_type,
                    cadd=hl.struct(
                        phred=ht.cadd.phred,
                        raw=ht.cadd.raw_score,
                    ),
                    alleleFrequencies=hl.set(
                        [f"{pop}-adj" for pop in self.populations]
                    ).map(
                        lambda p: hl.struct(
                            populationName=p,
                            alleleFrequency=ht.freq[ht.globals.freq_index_dict[p]].AF,
                        )
                    ),
                    vep=hl.struct(
                        mostSevereConsequence=ht.vep.most_severe_consequence,
                        transcriptConsequences=hl.map(
                            lambda x: hl.struct(
                                aminoAcids=x.amino_acids,
                                consequenceTerms=x.consequence_terms,
                                geneId=x.gene_id,
                                lof=x.lof,
                                polyphenScore=x.polyphen_score,
                                polyphenPrediction=x.polyphen_prediction,
                                siftScore=x.sift_score,
                                siftPrediction=x.sift_prediction,
                            ),
                            # Only keeping canonical transcripts
                            ht.vep.transcript_consequences.filter(
                                lambda x: (x.canonical == 1)
                                & (x.gene_symbol_source == "HGNC")
                            ),
                        ),
                    ),
                )
                .key_by("chromosome", "position")
                .drop("locus", "alleles")
                .select_globals()
                .to_spark(flatten=False)
            ),
            _schema=VariantAnnotation.get_schema(),
        )
