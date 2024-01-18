"""Quick and resilient framework to ingest genomic data for subsequent Spark processing."""

from __future__ import annotations

import gzip
import io
import math
import typing
from dataclasses import dataclass
from multiprocessing import Process, Queue
from multiprocessing.pool import Pool
from typing import Any

import pandas as pd
from resilient_fetch import ResilientFetch
from typing_extensions import Never


@dataclass
class PseudoConcat:
    """Efficient concatenation operations on Pandas arrays.

    For some tasks, you expect to receive data in blocks and then fetch the first X records at a time for processing.

    This requires concatenating large Pandas arrays, which is very inefficient because the concat operation creates

    This module implements a simple wrapper to operate on a list of Pandas dataframes as if they were one big dataframe, allowing for both adding and fetching the records very efficiently.
    """
    length = 0

    def __post_init__(self) -> None:
        """Initialise empty list of dataframes."""
        self.dfs: list[pd.DataFrame] = []

    def append(self, df: pd.DataFrame) -> None:
        """Add a new batch of records from a Pandas dataframe.

        Args:
            df (pd.DataFrame): a Pandas dataframe containing the records.
        """
        self.dfs.append(df)
        self.length += len(df)

    def get_leftmost(self, length: int) -> pd.DataFrame | None:
        """Fetch a specified number of leftmost (first) records and remove them from collection.

        Args:
            length (int): How many records to return.

        Returns:
            pd.DataFrame | None: a Pandas dataframe with a specified number of leftmost records; and None if there are no records in the collection.
        """
        if not self.dfs:
            return None
        dfs_to_output = []
        remaining_length = length
        while remaining_length and self.dfs:
            if remaining_length >= len(self.dfs[0]):
                dfs_to_output.append(self.dfs[0])
                remaining_length -= len(self.dfs[0])
                self.length -= len(self.dfs[0])
                self.dfs = self.dfs[1:]
            else:
                dfs_to_output.append(self.dfs[0][:remaining_length])
                self.dfs[0] = self.dfs[0][remaining_length:]
                self.length -= remaining_length
                remaining_length = 0
        return pd.concat(dfs_to_output, ignore_index=True)


def parse_data(
    lines_block: str,
    field_names: list[str],
    separator: str,
    chromosome_column_name: str,
) -> list[tuple[str, pd.DataFrame]]:
    """Parse a data block with complete lines into a Pandas dataframe.

    Args:
        lines_block (str): A text block containing complete lines.
        field_names (list[str]): A list containing field names for parsing the data.
        separator (str): Data field separator.
        chromosome_column_name (str): Name of the column containing chromosome.

    Returns:
        list[tuple[str, pd.DataFrame]]: a Pandas dataframe with parsed data.
    """
    assert field_names, "Field names are not specified."
    data_io = io.StringIO(lines_block)
    df = pd.read_csv(
        data_io,
        sep=separator,
        names=field_names,
        header=None,
        dtype=str,
    )
    grouped_data = df.groupby(chromosome_column_name, sort=False)
    return [
        (chromosome_id, chromosome_data.drop(columns=[chromosome_column_name]))
        for chromosome_id, chromosome_data in grouped_data
    ]


def write_parquet(
    data: tuple[str, int, pd.DataFrame],
    output_base_path: str,
) -> None:
    """Write a single Parquet file.

    Args:
        data(tuple[str, int, pd.DataFrame]): Tuple of current chromosome, chunk number, and data to emit.
        output_base_path (str): Output base path.
    """
    chromosome, chunk_number, df = data
    output_filename = (
        f"{output_base_path}/"
        f"chromosome={chromosome}/"
        f"part-{chunk_number:05}.snappy.parquet"
    )
    df.to_parquet(output_filename, compression="snappy")


@dataclass
class SparkPrep:
    """Fetch, decompress, parse, partition, and save the data."""

    # Configuration for step 1: fetching data from URI.
    # How many bytes of raw (uncompressed) data to fetch and parse at once.
    fetch_chunk_size = 100_000_000

    # Configuration for step 5: partitioning data blocks.
    # How many records, on average, to try and keep in each Parquet partition.
    emit_block_size = 1_000_000
    # How much of a look-ahead buffer to keep (times the `emit_block_size`)
    # Increasing this value increases memory footprint but decreases spread in the final parquet partitions.
    # 4.0 value means that an average error for the final partitions is ~5% and the maximum possible error is ~10%.
    emit_look_ahead_factor = 4.0
    # Denotes when the look-ahead buffer is long enough to emit a block from it.
    emit_ready_buffer = emit_block_size * (emit_look_ahead_factor + 1)

    # Processing parameters, to be set during class init.
    number_of_cores: int
    input_uri: str
    output_base_path: str
    separator: str
    chromosome_column_name: str
    drop_columns: list[str]

    # Data processing streams and parameters. Populated during post-init.
    data_stream = None
    field_names: list[str] | None = None

    def _cast_to_bytes(self, x: Any) -> typing.IO[bytes]:
        """Casts a given object to bytes. For rationale, see: https://stackoverflow.com/a/58407810.

        Args:
            x (Any): object to cast to bytes.

        Returns:
            typing.IO[bytes]: object cast to bytes.
        """
        return typing.cast(typing.IO[bytes], x)

    def _get_text_stream(self) -> io.TextIOWrapper:
        """Initialise and return a text stream ready for reading the data.

        Returns:
            io.TextIOWrapper: A text stream ready for reading the data.
        """
        gzip_stream = self._cast_to_bytes(ResilientFetch(self.input_uri))
        bytes_stream = self._cast_to_bytes(gzip.GzipFile(fileobj=gzip_stream))
        text_stream = io.TextIOWrapper(bytes_stream)
        return text_stream

    def __post_init__(self) -> None:
        """Post init step."""
        # Set up default values.
        if not self.drop_columns:
            self.drop_columns = []

        # Detect field names.
        self.field_names = (
            self._get_text_stream().readline().rstrip().split(self.separator)
        )

    def _emit_complete_line_blocks(
        self,
        q_out: list[Queue[str | None]],
    ) -> None:
        """Given text blocks, emit blocks which contain complete text lines.

        Args:
            q_out (list[Queue[str | None]]): List of output queues.
        """
        # Initialise buffer for storing incomplete lines.
        buffer = ""
        queue_index = 0

        # Initialise text stream.
        text_stream = self._get_text_stream()
        # Skip header line.
        text_stream.readline()

        # Process data.
        while True:
            # Get more data from the input queue.
            # text_block = q_in.get()
            text_block = text_stream.read(self.fetch_chunk_size)
            if text_block:
                # Process text block.
                buffer += text_block
                # Find the rightmost newline so that we always emit blocks of complete records.
                rightmost_newline_split = buffer.rfind("\n") + 1
                q_out[queue_index].put(buffer[:rightmost_newline_split])
                # Update buffer.
                buffer = buffer[rightmost_newline_split:]
                # Switch to next queue.
                queue_index = (queue_index + 1) % len(q_out)
            else:
                # We have reached end of stream. Because buffer only contains *incomplete* lines, it should be empty.
                assert (
                    not buffer
                ), "Expected buffer to be empty at the end of stream, but incomplete lines are found."
                for q in q_out:
                    q.put(None)
                break

    def _parse_data(
        self,
        q_in: Queue[str | None],
        q_out: Queue[list[tuple[str, pd.DataFrame]] | None],
    ) -> None:
        """Parse complete-line data blocks into Pandas dataframes, utilising multiple workers.

        Args:
            q_in (Queue[str | None]): Input queue with complete-line text data blocks.
            q_out (Queue[list[tuple[str, pd.DataFrame]] | None]): List of output queues with Pandas dataframes split by chromosome.
        """
        while True:
            lines_block = q_in.get()
            if lines_block is not None:
                parsed_data = parse_data(
                    lines_block,
                    typing.cast(list[str], self.field_names),
                    self.separator,
                    self.chromosome_column_name,
                )
                q_out.put(parsed_data)
            else:
                q_out.put(None)
                break

    def _partition_data(
        self,
        q_in: list[Queue[list[tuple[str, pd.DataFrame]] | None]],
    ) -> None:
        """Process stream of data blocks and partition them.

        Args:
            q_in (list[Queue[list[tuple[str, pd.DataFrame]] | None]]): List of input queues with Pandas dataframes split by chromosome.
        """
        # Initialise counters.
        current_chromosome = ""
        current_block_index = 0
        current_data_block = PseudoConcat()

        # Initialise input queues.
        queue_index = 0

        # Initialise pool for producing Parquet files.
        pool = Pool(self.number_of_cores)

        # Process blocks.
        while True:
            # Get more data from the queue.
            df_blocks = q_in[queue_index].get()
            if df_blocks is None:
                # If we reached end of stream, formulate a special value which will cause all processing to properly wrap up.
                df_blocks = [("", None)]
            else:
                # And if we have not, we should switch to the next queue.
                queue_index = (queue_index + 1) % len(q_in)

            # We now process all chromosome blocks.
            for df_block_by_chromosome in df_blocks:
                chromosome_id, chromosome_data = df_block_by_chromosome

                # If this is the first block we ever see, initialise "current_chromosome".
                if not current_chromosome:
                    current_chromosome = chromosome_id

                # If chromosome has changed (including to "" incicating end of stream), we need to emit the previous one.
                if chromosome_id != current_chromosome:
                    # Calculate the optimal number of blocks.
                    number_of_blocks = max(
                        round(current_data_block.length / self.emit_block_size), 1
                    )
                    records_per_block = math.ceil(
                        current_data_block.length / number_of_blocks
                    )
                    # Emit remaining blocks one by one.
                    for _ in range(number_of_blocks):
                        pool.apply_async(
                            write_parquet,
                            [
                                (
                                    current_chromosome,
                                    current_block_index,
                                    current_data_block.get_leftmost(records_per_block),
                                ),
                                self.output_base_path,
                            ],
                        )
                        current_block_index += 1
                    # Reset everything for the next chromosome.
                    current_chromosome = chromosome_id
                    current_block_index = 0
                    current_data_block = PseudoConcat()

                if current_chromosome:
                    # We have a new chromosome to process.
                    # We should now append new data to the chromosome buffer.
                    current_data_block.append(chromosome_data)
                    # If we have enough data for the chromosome we are currently processing, we can emit some blocks already.
                    while current_data_block.length >= self.emit_ready_buffer:
                        emit_block = current_data_block.get_leftmost(
                            self.emit_block_size
                        )
                        pool.apply_async(
                            write_parquet,
                            [
                                (current_chromosome, current_block_index, emit_block),
                                self.output_base_path,
                            ],
                        )
                        current_block_index += 1
                else:
                    # We have reached end of stream.
                    pool.close()
                    pool.join()
                    return

    def process(self) -> None:
        """Process one input file start to finish.

        Raises:
            Exception: if one of the processes raises an exception.
        """
        # Set up queues for process exchange.
        q_in: list[Queue[Never]] = [
            Queue(maxsize=4) for _ in range(self.number_of_cores)
        ]
        q_out: list[Queue[Never]] = [
            Queue(maxsize=4) for _ in range(self.number_of_cores)
        ]
        read_process = Process(
            target=self._emit_complete_line_blocks,
            args=(q_in,),
        )
        parser_processes = [
            Process(
                target=self._parse_data,
                args=(q_in[i], q_out[i]),
            )
            for i in range(self.number_of_cores)
        ]
        write_process = Process(
            target=self._partition_data,
            args=(q_out,),
        )
        processes = [read_process] + parser_processes + [write_process]
        # Start all processes.
        for p in processes:
            p.start()
        # Keep checking if any of the processes has completed or raised an exception.
        while True:
            anyone_alive = False
            for p in processes:
                p.join(timeout=5)
                if p.is_alive():
                    anyone_alive = True
                elif p.exitcode != 0:
                    raise Exception(f"Process {p} has failed.")
            if not anyone_alive:
                break
