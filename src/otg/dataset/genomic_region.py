"""Class to model and validate genomic region."""
from __future__ import annotations

from dataclasses import dataclass


@dataclass
class GenomicRegion:
    """The GenomicRegion class represents a region of a genome."""

    chromosome: str
    start: int
    end: int

    def __post_init__(self: GenomicRegion) -> None:
        """Force validation after initialization."""
        # Validate:
        self.validate()

    @classmethod
    def from_string(cls: type[GenomicRegion], region: str) -> GenomicRegion:
        """Parse region string to chr:start-end.

        Args:
            region (str): Genomic region expected to follow chr##:#,###-#,### format or ##:####-#####.

        Raises:
            ValueError: If the end and start positions cannot be casted to integer or not all
                three values value error is raised.

        Returns:
            GenomicRegion: representing Chromosome, start position, end position

        Examples:
            >>> GenomicRegion.from_string('chr6:28,510,120-33,480,577')
            GenomicRegion(chromosome='6', start=28510120, end=33480577)
            >>> GenomicRegion.from_string('6:28510120-33480577')
            GenomicRegion(chromosome='6', start=28510120, end=33480577)
            >>> GenomicRegion.from_string('6:28510120')
            Traceback (most recent call last):
                ...
            ValueError: Genomic region should follow a ##:####-#### format.
            >>> GenomicRegion.from_string('6:28510120-foo')
            Traceback (most recent call last):
                ...
            ValueError: Start and the end position of the region has to be integer.
        """
        region = region.replace(":", "-").replace(",", "")
        try:
            (chromosome, start_position, end_position) = region.split("-")
        except ValueError as err:
            raise ValueError(
                "Genomic region should follow a ##:####-#### format."
            ) from err

        try:
            chromosome = chromosome.replace("chr", "")
            start = int(start_position)
            end = int(end_position)

        except ValueError as err:
            raise ValueError(
                "Start and the end position of the region has to be integer."
            ) from err

        # Initialize region object:
        return GenomicRegion(chromosome=chromosome, start=start, end=end)

    def validate(self: GenomicRegion) -> None:
        """As part of the initialization process, genomic region is validated.

        Raises:
            ValueError: raised if:
                1. The chromosome is not string
                2. The start and end positions are not integers.
                3. If the start position is not positive.
                4. If end position is not greater than start.

        Examples:
            >>> GenomicRegion.from_string('chr6:28,510,120-33,480,577').validate()
            >>> GenomicRegion(1, 2, 3)
            Traceback (most recent call last):
                ...
            ValueError: Chromosome has to be a string.
            >>> GenomicRegion('1', 2, "foo")
            Traceback (most recent call last):
                ...
            ValueError: End position has to be an integer.
            >>> GenomicRegion('1', -2, 3)
            Traceback (most recent call last):
                ...
            ValueError: Start position has to be a positive integer.
            >>> GenomicRegion('1', 12, 3)
            Traceback (most recent call last):
                ...
            ValueError: End position has to be bigger than start position.
        """
        # Making sure things are the right type
        try:
            assert isinstance(self.chromosome, str)
        except AssertionError as e:
            raise ValueError("Chromosome has to be a string.") from e

        try:
            assert isinstance(self.start, int)
        except AssertionError as e:
            raise ValueError("Start position has to be an integer.") from e
        try:
            assert isinstance(self.end, int)
        except AssertionError as e:
            raise ValueError("End position has to be an integer.") from e

        # Asserting start:
        try:
            assert self.start >= 0
        except AssertionError as e:
            raise ValueError("Start position has to be a positive integer.") from e
        try:
            assert self.start < self.end
        except AssertionError as e:
            raise ValueError(
                "End position has to be bigger than start position."
            ) from e

    def __len__(self: GenomicRegion) -> int:
        """Return length of the region.

        Returns:
            int: legth in basepairs region spans.

        Examples:
            >>> r = GenomicRegion("2", 10, 20)
            >>> len(r)
            10
        """
        return self.end - self.start
