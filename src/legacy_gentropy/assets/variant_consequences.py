"""Common module representing Ensembl Variation Variant Consequences.

This module contains the `Consequence` dataclass and the `VariantConsequence` enum.
The `VariantConsequence` enum contains all `Consequence` instances defined by the Ensembl Variation API (used by Variant Effect Predictor - VEP).
The full definition of the consequence was derived from the Ensembl Variation API.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


@dataclass(frozen=True)
class Consequence:
    """Base class for the variant consequence term.

    Note:
        This class is used as a base class for the `VariantConsequence` enum, which
        contains all the valid consequence terms defined by the Ensembl Variation API.

    Warning:
        **Building new instances of this class is not recommended**, as it may lead to inconsistencies
        in the way the `Consequence.score` is calculated.
        Rather then creating new instances of this class it is recommended to subclass it
        and override the `score` property with custom logic.

    Examples:
        >>> c = VariantConsequence.MISSENSE_VARIANT.value
        >>> c.id
        'SO_0001583'
        >>> c.label
        'missense_variant'
        >>> c.impact
        'MODERATE'
        >>> c.score
        0.68
        >>> c.rank
        13
        >>> str(c)
        'Consequence(id=SO_0001583, label=missense_variant, impact=MODERATE, rank=13)'
    """

    id: str
    label: str
    impact: str
    rank: int

    @property
    def score(self) -> float:
        r"""Scores the impact of a variant consequence.

        Note:
            The consequence scores are derived from the rank introduced by the `ensembl-variation` ranking of sequence ontology consequence terms.
            The ranking is derived from [Constants.pm](https://github.com/Ensembl/ensembl-variation/blob/ac9116d964b33253cb34f727cad8c1c022d28672/modules/Bio/EnsEMBL/Variation/Utils/Constants.pm#L375).
            The scoring that follows the inverse of the ranking is based on the formula.
            ```math
            score=1 - \frac{rank}{max(rank)}
            ```
            where $max(rank)$ is the maximum rank of the consequences (41 in this case).
            The score is then rounded to 2 decimal places.

        * The score derived this way follows the VEP consequence ranking, which means that the consequence score is in sync to the actual mostSevereConsequence term
        * The score is **different for each consequence term**
        * The score follow the severity measure (0.98 - highest severity, 0 - lowest severity)
        """
        return round(1 - (self.rank / len(VariantConsequence)), 2)

    def __str__(self) -> str:
        """String representation of the consequence.

        Returns:
            str: String representation of the consequence.
        """
        return f"Consequence(id={self.id}, label={self.label}, impact={self.impact}, rank={self.rank})"

    def __repr__(self) -> str:
        """Official string representation of the consequence.

        Returns:
            str: String representation of the consequence.
        """
        return self.__str__()


class VariantConsequence(Enum):
    """Enum representing Ensembl Variation Variant Consequences.

    This enum contains all `Consequence` instances defined by the Ensembl Variation API (used by Variant Effect Predictor - VEP).

    The full definition of the consequence was derived from the Ensembl Variation API.
    See [issue](https://github.com/opentargets/issues/issues/3952) for more details.

    Attributes:
        TRANSCRIPT_ABLATION (Consequence): A feature ablation whereby the deleted region includes a transcript feature
        SPLICE_ACCEPTOR_VARIANT (Consequence): A splice variant that changes the 2 base region at the 3' end of an intron
        SPLICE_DONOR_VARIANT (Consequence): A splice variant that changes the 2 base region at the 5' end of an intron
        STOP_GAINED (Consequence): A sequence variant whereby at least one base of a codon is changed, resulting in a premature stop codon, leading to a shortened transcript
        FRAMESHIFT_VARIANT (Consequence): A sequence variant which causes a disruption of the translational reading frame, because the number of nucleotides inserted or deleted is not a multiple of three
        STOP_LOST (Consequence): A sequence variant where at least one base of the terminator codon (stop) is changed, resulting in an elongated transcript
        START_LOST (Consequence): A codon variant that changes at least one base of the canonical start codon
        TRANSCRIPT_AMPLIFICATION (Consequence): A feature amplification of a region containing a transcript
        FEATURE_ELONGATION (Consequence): A sequence variant that causes the extension of a genomic feature, with regard to the reference sequence
        FEATURE_TRUNCATION (Consequence): A sequence variant that causes the reduction of a genomic feature, with regard to the reference sequence
        INFRAME_INSERTION (Consequence): An inframe non synonymous variant that inserts bases into in the coding sequence
        INFRAME_DELETION (Consequence): An inframe non synonymous variant that deletes bases from the coding sequence
        MISSENSE_VARIANT (Consequence): A sequence variant, that changes one or more bases, resulting in a different amino acid sequence but where the length is preserved
        PROTEIN_ALTERING_VARIANT (Consequence): A sequence_variant which is predicted to change the protein encoded in the coding sequence
        SPLICE_DONOR_5TH_BASE_VARIANT (Consequence): A sequence variant that causes a change at the 5th base pair after the start of the intron in the orientation of the transcript
        SPLICE_REGION_VARIANT (Consequence): A sequence variant in which a change has occurred within the region of the splice site, either within 1-3 bases of the exon or 3-8 bases of the intron
        SPLICE_DONOR_REGION_VARIANT (Consequence): A sequence variant that falls in the region between the 3rd and 6th base after splice junction (5' end of intron).
        SPLICE_POLYPYRIMIDINE_TRACT_VARIANT (Consequence): A sequence variant that falls in the polypyrimidine tract at 3' end of intron between 17 and 3 bases from the end (acceptor -3 to acceptor -17)
        INCOMPLETE_TERMINAL_CODON_VARIANT (Consequence): A sequence variant where at least one base of the final codon of an incompletely annotated transcript is changed
        START_RETAINED_VARIANT (Consequence): A sequence variant where at least one base in the start codon is changed, but the start remains
        STOP_RETAINED_VARIANT (Consequence): A sequence variant where at least one base in the terminator codon is changed, but the terminator remains
        SYNONYMOUS_VARIANT (Consequence): A sequence variant where there is no resulting change to the encoded amino acid
        CODING_SEQUENCE_VARIANT (Consequence): A sequence variant that changes the coding sequence
        MATURE_MIRNA_VARIANT (Consequence): A transcript variant located with the sequence of the mature miRNA
        5_PRIME_UTR_VARIANT (Consequence): A UTR variant of the 5' UTR
        3_PRIME_UTR_VARIANT (Consequence): A UTR variant of the 3' UTR
        NON_CODING_TRANSCRIPT_EXON_VARIANT (Consequence): A sequence variant that changes non-coding exon sequence in a non-coding transcript
        INTRON_VARIANT (Consequence): A transcript variant occurring within an intron
        NMD_TRANSCRIPT_VARIANT (Consequence): A variant in a transcript that is the target of NMD
        NON_CODING_TRANSCRIPT_VARIANT (Consequence): A transcript variant of a non coding RNA gene
        CODING_TRANSCRIPT_VARIANT (Consequence): A transcript variant of a protein coding gene
        UPSTREAM_GENE_VARIANT (Consequence): A sequence variant located 5' of a gene
        DOWNSTREAM_GENE_VARIANT (Consequence): A sequence variant located 3' of a gene
        TFBS_ABLATION (Consequence): A feature ablation whereby the deleted region includes a transcription factor binding site
        TFBS_AMPLIFICATION (Consequence): A feature amplification of a region containing a transcription factor binding site
        TF_BINDING_SITE_VARIANT (Consequence): A sequence variant located within a transcription factor binding site
        REGULATORY_REGION_ABLATION (ConsequeSO_0001893nce): A feature ablation whereby the deleted region includes a regulatory region
        REGULATORY_REGION_AMPLIFICATION (Consequence): A feature amplification of a region containing a regulatory region
        REGULATORY_REGION_VARIANT (Consequence): A sequence variant located within a regulatory region
        INTERGENIC_VARIANT (Consequence): A sequence variant located in the intergenic region, between genes
        SEQUENCE_VARIANT (Consequence): A sequence_variant is a non exact copy of a sequence_feature or genome exhibiting one or more sequence_alterations
    """

    TRANSCRIPT_ABLATION = Consequence(
        id="SO_0001893", label="transcript_ablation", impact="HIGH", rank=1
    )
    SPLICE_ACCEPTOR_VARIANT = Consequence(
        id="SO_0001574", label="splice_acceptor_variant", impact="HIGH", rank=2
    )
    SPLICE_DONOR_VARIANT = Consequence(
        id="SO_0001575", label="splice_donor_variant", impact="HIGH", rank=3
    )
    STOP_GAINED = Consequence(
        id="SO_0001587", label="stop_gained", impact="HIGH", rank=4
    )
    FRAMESHIFT_VARIANT = Consequence(
        id="SO_0001589", label="frameshift_variant", impact="HIGH", rank=5
    )
    STOP_LOST = Consequence(id="SO_0001578", label="stop_lost", impact="HIGH", rank=6)
    START_LOST = Consequence(id="SO_0002012", label="start_lost", impact="HIGH", rank=7)
    TRANSCRIPT_AMPLIFICATION = Consequence(
        id="SO_0001889", label="transcript_amplification", impact="HIGH", rank=8
    )
    FEATURE_ELONGATION = Consequence(
        id="SO_0001907", label="feature_elongation", impact="HIGH", rank=9
    )
    FEATURE_TRUNCATION = Consequence(
        id="SO_0001906", label="feature_truncation", impact="HIGH", rank=10
    )
    INFRAME_INSERTION = Consequence(
        id="SO_0001821", label="inframe_insertion", impact="MODERATE", rank=11
    )
    INFRAME_DELETION = Consequence(
        id="SO_0001822", label="inframe_deletion", impact="MODERATE", rank=12
    )
    MISSENSE_VARIANT = Consequence(
        id="SO_0001583", label="missense_variant", impact="MODERATE", rank=13
    )
    PROTEIN_ALTERING_VARIANT = Consequence(
        id="SO_0001818", label="protein_altering_variant", impact="MODERATE", rank=14
    )
    SPLICE_DONOR_5TH_BASE_VARIANT = Consequence(
        id="SO_0001787", label="splice_donor_5th_base_variant", impact="LOW", rank=15
    )
    SPLICE_REGION_VARIANT = Consequence(
        id="SO_0001630", label="splice_region_variant", impact="LOW", rank=16
    )
    SPLICE_DONOR_REGION_VARIANT = Consequence(
        id="SO_0002170", label="splice_donor_region_variant", impact="LOW", rank=17
    )
    SPLICE_POLYPYRIMIDINE_TRACT_VARIANT = Consequence(
        id="SO_0002169",
        label="splice_polypyrimidine_tract_variant",
        impact="LOW",
        rank=18,
    )
    INCOMPLETE_labelINAL_CODON_VARIANT = Consequence(
        id="SO_0001626",
        label="incomplete_labelinal_codon_variant",
        impact="LOW",
        rank=19,
    )
    START_RETAINED_VARIANT = Consequence(
        id="SO_0002019", label="start_retained_variant", impact="LOW", rank=20
    )
    STOP_RETAINED_VARIANT = Consequence(
        id="SO_0001567", label="stop_retained_variant", impact="LOW", rank=21
    )
    SYNONYMOUS_VARIANT = Consequence(
        id="SO_0001819", label="synonymous_variant", impact="LOW", rank=22
    )
    CODING_SEQUENCE_VARIANT = Consequence(
        id="SO_0001580", label="coding_sequence_variant", impact="MODIFIER", rank=23
    )
    MATURE_MIRNA_VARIANT = Consequence(
        id="SO_0001620", label="mature_miRNA_variant", impact="MODIFIER", rank=24
    )
    FIVE_PRIME_UTR_VARIANT = Consequence(
        id="SO_0001623", label="5_prime_UTR_variant", impact="MODIFIER", rank=25
    )
    THREE_PRIME_UTR_VARIANT = Consequence(
        id="SO_0001624", label="3_prime_UTR_variant", impact="MODIFIER", rank=26
    )
    NON_CODING_TRANSCRIPT_EXON_VARIANT = Consequence(
        id="SO_0001792",
        label="non_coding_transcript_exon_variant",
        impact="MODIFIER",
        rank=27,
    )
    INTRON_VARIANT = Consequence(
        id="SO_0001627", label="intron_variant", impact="MODIFIER", rank=28
    )
    NMD_TRANSCRIPT_VARIANT = Consequence(
        id="SO_0001621", label="NMD_transcript_variant", impact="MODIFIER", rank=29
    )
    NON_CODING_TRANSCRIPT_VARIANT = Consequence(
        id="SO_0001619",
        label="non_coding_transcript_variant",
        impact="MODIFIER",
        rank=30,
    )
    CODING_TRANSCRIPT_VARIANT = Consequence(
        id="SO_0001968", label="coding_transcript_variant", impact="MODIFIER", rank=31
    )
    UPSTREAM_GENE_VARIANT = Consequence(
        id="SO_0001631", label="upstream_gene_variant", impact="MODIFIER", rank=32
    )
    DOWNSTREAM_GENE_VARIANT = Consequence(
        id="SO_0001632", label="downstream_gene_variant", impact="MODIFIER", rank=33
    )
    TFBS_ABLATION = Consequence(
        id="SO_0001895", label="TFBS_ablation", impact="MODERATE", rank=34
    )
    TFBS_AMPLIFICATION = Consequence(
        id="SO_0001892", label="TFBS_amplification", impact="MODIFIER", rank=35
    )
    TF_BINDING_SITE_VARIANT = Consequence(
        id="SO_0001782", label="TF_binding_site_variant", impact="MODIFIER", rank=36
    )
    REGULATORY_REGION_ABLATION = Consequence(
        id="SO_0001894", label="regulatory_region_ablation", impact="MODIFIER", rank=37
    )
    REGULATORY_REGION_AMPLIFICATION = Consequence(
        id="SO_0001891",
        label="regulatory_region_amplification",
        impact="MODIFIER",
        rank=38,
    )
    REGULATORY_REGION_VARIANT = Consequence(
        id="SO_0001566", label="regulatory_region_variant", impact="MODIFIER", rank=39
    )
    INTERGENIC_VARIANT = Consequence(
        id="SO_0001628", label="intergenic_variant", impact="MODIFIER", rank=40
    )
    SEQUENCE_VARIANT = Consequence(
        id="SO_0001060", label="sequence_variant", impact="MODIFIER", rank=41
    )

    @classmethod
    def map_sequence_ontology(cls) -> dict[str, str]:
        """Return the mapping of the `Consequence.label` (key) and `Consequence.id` (value) representing Sequence Ontology term.

        Returns:
            dict[str, str]: Mapping of consequence label to ID.

        Examples:
            >>> m = VariantConsequence.map_sequence_ontology()
            >>> m["missense_variant"]
            'SO_0001583'
            >>> len(m)
            41
        """
        return {
            consequence.value.label: consequence.value.id
            for consequence in cls.__members__.values()
        }

    @classmethod
    def map_score(cls) -> dict[str, float]:
        """Return the mapping of the `Consequence.label` (key) and `Consequence.score` (value).

        Returns:
            dict[str, float]: Mapping of consequence label to score.

        Examples:
            >>> s = VariantConsequence.map_score()
            >>> s["missense_variant"]
            0.68
            >>> len(s)
            41
        """
        return {
            consequence.value.label: consequence.value.score
            for consequence in cls.__members__.values()
        }
