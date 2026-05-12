"""Step to generate interval annotation dataset."""

from __future__ import annotations

from typing import Annotated

from pydantic import BaseModel, Field

from gentropy.common.session import Session
from gentropy.dataset.biosample_index import BiosampleIndex
from gentropy.dataset.contig_index import ContigIndex
from gentropy.dataset.target_index import TargetIndex
from gentropy.datasource.intervals.e2g import IntervalsE2G
from gentropy.datasource.intervals.epiraction import IntervalsEpiraction


class IntervalE2GDefaults(BaseModel, frozen=True):
    """Defaults for IntervalE2GStep.

    All fields are mandatory input/output paths - no defaults.
    """

    target_index_path: Annotated[str, Field(description="Input target index path.")]
    biosample_mapping_path: Annotated[
        str, Field(description="Input biosample mapping path.")
    ]
    biosample_index_path: Annotated[
        str, Field(description="Input biosample index path.")
    ]
    chromosome_contig_index_path: Annotated[
        str, Field(description="Input chromosome contig index path.")
    ]
    interval_source: Annotated[str, Field(description="Input intervals source path.")]
    valid_output_path: Annotated[str, Field(description="Output valid intervals path.")]
    invalid_output_path: Annotated[
        str, Field(description="Output invalid intervals path.")
    ]
    invalid_qc_reasons: Annotated[
        list[str],
        Field(
            default_factory=list,
            description="List of invalid quality check reason names from `IntervalQualityCheck`.",
        ),
    ]
    min_valid_score: Annotated[
        float, Field(description="Minimum valid score for interval QC.")
    ] = 0.6
    max_valid_score: Annotated[
        float, Field(description="Maximum valid score for interval QC.")
    ] = 1.0


class IntervalE2GStep:
    """Interval E2G step.

    This step generates a dataset that contains interval evidence supporting the functional associations of variants with genes.
    """

    def __init__(
        self,
        config: IntervalE2GDefaults,
        session: Session,
    ) -> None:
        """Run intervals step.

        Args:
            config: Step configuration defaults.
            session: Session object.
        """
        invalid_qc_reasons = list(config.invalid_qc_reasons)

        biosample_mapping = session.spark.read.csv(
            config.biosample_mapping_path, header=True
        )
        target_index = TargetIndex.from_parquet(
            session, config.target_index_path
        ).persist()
        biosample_index = BiosampleIndex.from_parquet(
            session, config.biosample_index_path
        )
        contig_index = ContigIndex.from_parquet(
            session, config.chromosome_contig_index_path
        )
        data = IntervalsE2G.read(session.spark, config.interval_source)
        interval_e2g = IntervalsE2G.parse(data, biosample_mapping, target_index)
        valid, invalid = interval_e2g.qc(
            contig_index=contig_index,
            target_index=target_index,
            biosample_index=biosample_index,
            min_valid_score=config.min_valid_score,
            max_valid_score=config.max_valid_score,
            invalid_qc_reasons=invalid_qc_reasons,
        )
        (
            valid.df.repartitionByRange(
                session.output_partitions, "chromosome", "start"
            )
            .write.mode(session.write_mode)
            .parquet(config.valid_output_path)
        )
        (
            invalid.df.repartitionByRange(
                session.output_partitions, "chromosome", "start"
            )
            .write.mode(session.write_mode)
            .parquet(config.invalid_output_path)
        )


class IntervalEpiractionDefaults(BaseModel, frozen=True):
    """Defaults for IntervalEpiractionStep."""

    target_index_path: Annotated[str, Field(description="Input target index path.")]
    biosample_index_path: Annotated[
        str, Field(description="Input biosample index path.")
    ]
    chromosome_contig_index_path: Annotated[
        str, Field(description="Input chromosome contig index path.")
    ]
    interval_source: Annotated[str, Field(description="Input intervals source path.")]
    valid_output_path: Annotated[str, Field(description="Output valid intervals path.")]
    invalid_output_path: Annotated[
        str, Field(description="Output invalid intervals path.")
    ]
    min_valid_score: Annotated[
        float, Field(description="Minimum valid score for interval QC.")
    ] = 0.6
    max_valid_score: Annotated[
        float, Field(description="Maximum valid score for interval QC.")
    ] = 1.0
    invalid_qc_reasons: Annotated[
        list[str],
        Field(
            default_factory=list,
            description="List of invalid quality check reason names from `IntervalQualityCheck`.",
        ),
    ]


class IntervalEpiractionStep:
    """Interval epiraction step.

    This step generates a dataset that contains interval evidence supporting the functional associations of variants with genes.

    """

    def __init__(
        self,
        config: IntervalEpiractionDefaults,
        session: Session,
    ) -> None:
        """Run intervals step.

        Args:
            config: Step configuration defaults.
            session: Session object.
        """
        invalid_qc_reasons = list(config.invalid_qc_reasons)
        target_index = TargetIndex.from_parquet(
            session, config.target_index_path
        ).persist()
        data = IntervalsEpiraction.read(session.spark, config.interval_source)
        interval_epiraction = IntervalsEpiraction.parse(data, target_index)
        biosample_index = BiosampleIndex.from_parquet(
            session, config.biosample_index_path
        )
        contig_index = ContigIndex.from_parquet(
            session, config.chromosome_contig_index_path
        )
        valid, invalid = interval_epiraction.qc(
            contig_index=contig_index,
            target_index=target_index,
            biosample_index=biosample_index,
            min_valid_score=config.min_valid_score,
            max_valid_score=config.max_valid_score,
            invalid_qc_reasons=invalid_qc_reasons,
        )
        (
            valid.df.repartitionByRange(
                session.output_partitions, "chromosome", "start"
            )
            .write.mode(session.write_mode)
            .parquet(config.valid_output_path)
        )
        (
            invalid.df.repartitionByRange(
                session.output_partitions, "chromosome", "start"
            )
            .write.mode(session.write_mode)
            .parquet(config.invalid_output_path)
        )
