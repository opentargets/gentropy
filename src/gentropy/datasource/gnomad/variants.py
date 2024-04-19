"""Import gnomAD variants dataset."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import hail as hl

from gentropy.dataset.variant_annotation import VariantAnnotation

if TYPE_CHECKING:
    pass


@dataclass
class GnomADVariants:
    """GnomAD variants included in the GnomAD genomes dataset.

    Attributes:
        gnomad_genomes (str): Path to gnomAD genomes hail table. Defaults to gnomAD's 4.0 release.
        chain_hail_38_37 (str): Path to GRCh38 to GRCh37 chain file. Defaults to Hail's chain file.
        populations (list[str]): List of populations to include. Defaults to all populations.
    """

    gnomad_genomes: str = "gs://gcp-public-data--gnomad/release/4.0/ht/genomes/gnomad.genomes.v4.0.sites.ht/"
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
            "remaining",  # Other
        ]
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
                ht.select(
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
                    chromosomeB37=ht.locus_GRCh37.contig.replace("chr", ""),
                    positionB37=ht.locus_GRCh37.position,
                    referenceAllele=ht.alleles[0],
                    alternateAllele=ht.alleles[1],
                    rsIds=ht.rsid,
                    alleleType=ht.allele_info.allele_type,
                    alleleFrequencies=hl.set(
                        [f"{pop}_adj" for pop in self.populations]
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
                            ),
                            # Only keeping canonical transcripts
                            ht.vep.transcript_consequences.filter(
                                lambda x: (x.canonical == 1)
                                & (x.gene_symbol_source == "HGNC")
                            ),
                        ),
                    ),
                    inSilicoPredictors=hl.struct(
                        cadd=hl.struct(
                            phred=ht.in_silico_predictors.cadd.phred,
                            raw=ht.in_silico_predictors.cadd.raw_score,
                        ),
                        revelMax=ht.in_silico_predictors.revel_max,
                        spliceaiDsMax=ht.in_silico_predictors.spliceai_ds_max,
                        pangolinLargestDs=ht.in_silico_predictors.pangolin_largest_ds,
                        phylop=ht.in_silico_predictors.phylop,
                        siftMax=ht.in_silico_predictors.sift_max,
                        polyphenMax=ht.in_silico_predictors.polyphen_max,
                    ),
                )
                .key_by("chromosome", "position")
                .drop("locus", "alleles")
                .select_globals()
                .to_spark(flatten=False)
            ),
            _schema=VariantAnnotation.get_schema(),
        )
