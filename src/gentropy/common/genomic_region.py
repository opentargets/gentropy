"""Genomic Region class."""

from enum import Enum


class KnownGenomicRegions(Enum):
    """Known genomic regions in the human genome in string format."""

    MHC = "chr6:25726063-33400556"


class GenomicRegion:
    """Genomic regions of interest.

    Attributes:
        chromosome (str): Chromosome.
        start (int): Start position.
        end (int):
    """

    def __init__(self, chromosome: str, start: int, end: int) -> None:
        """Class constructor.

        Args:
            chromosome (str): Chromosome.
            start (int): Start position.
            end (int): End position.
        """
        self.chromosome = chromosome
        self.start = start
        self.end = end

    def __str__(self) -> str:
        """String representation of the genomic region.

        Returns:
            str: Genomic region in chr:start-end format.
        """
        return f"{self.chromosome}:{self.start}-{self.end}"

    @classmethod
    def from_string(cls: type["GenomicRegion"], region: str) -> "GenomicRegion":
        """Parse region string to chr:start-end.

        Args:
            region (str): Genomic region expected to follow chr##:#,###-#,### format or ##:####-#####.

        Returns:
            GenomicRegion: Genomic region object.

        Raises:
            ValueError: If the end and start positions cannot be casted to integer or not all three values value error is raised.

        Examples:
            >>> print(GenomicRegion.from_string('chr6:28,510,120-33,480,577'))
            6:28510120-33480577
            >>> print(GenomicRegion.from_string('6:28510120-33480577'))
            6:28510120-33480577
            >>> print(GenomicRegion.from_string('6:28510120'))
            Traceback (most recent call last):
                ...
            ValueError: Genomic region should follow a ##:####-#### format.
            >>> print(GenomicRegion.from_string('6:28510120-foo'))
            Traceback (most recent call last):
                ...
            ValueError: Start and the end position of the region has to be integer.
        """
        region = region.replace(":", "-").replace(",", "")
        try:
            chromosome, start_position, end_position = region.split("-")
        except ValueError as err:
            raise ValueError(
                "Genomic region should follow a ##:####-#### format."
            ) from err

        try:
            return cls(
                chromosome=chromosome.replace("chr", ""),
                start=int(start_position),
                end=int(end_position),
            )
        except ValueError as err:
            raise ValueError(
                "Start and the end position of the region has to be integer."
            ) from err

    @classmethod
    def from_known_genomic_region(
        cls: type["GenomicRegion"], region: KnownGenomicRegions
    ) -> "GenomicRegion":
        """Get known genomic region.

        Args:
            region (KnownGenomicRegions): Known genomic region.

        Returns:
            GenomicRegion: Genomic region object.

        Examples:
            >>> print(GenomicRegion.from_known_genomic_region(KnownGenomicRegions.MHC))
            6:25726063-33400556
        """
        return GenomicRegion.from_string(region.value)
