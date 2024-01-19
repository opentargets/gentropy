---
title: Create a dataset
---

Gentropy provides a collection of `Dataset`s that encapsulate key concepts in the field of genetics. For example, to represent summary statistics, you'll use the [`SummaryStatistics`](../../python_api/datasets/summary_statistics.md) class. This datatype comes with a set of useful operations to disentangle the genetic architecture of a trait or disease.

!!! info "Any instance of Dataset will have 2 common attributes"

    - **df**: the Spark DataFrame that contains the data
    - **schema**: the definition of the data structure in Spark format

In this section you'll learn the different ways of how to create a `Dataset` instances.

## Creating a dataset from parquet

All the `Dataset`s have a `from_parquet` method that allows you to create any `Dataset` instance from a parquet file or directory.

```python
from gentropy.datasets.summary_statistics import SummaryStatistics

# Create a SummaryStatistics object by loading data from the specified path
path = "path/to/summary/stats"
summary_stats = SummaryStatistics.from_parquet(session, path)
```

!!! info "Parquet files"

    Parquet is a columnar storage format that is widely used in the Spark ecosystem. It is the recommended format for storing large datasets. For more information about parquet, please visit [https://parquet.apache.org/](https://parquet.apache.org/).

## Creating a dataset from a data source

Alternatively, `Dataset`s can be created using a [data source](../../python_api/datasources/_datasources.md) harmonisation method. For example, to create a `SummaryStatistics` object from Finngen's raw summary statistics, you can use the [`FinnGen`](../../python_api/datasources/finngen/summary_stats.md) data source.

```python
from gentropy.datasources.finngen.summary_stats import FinnGenSummaryStats

# Create a SummaryStatistics object by loading raw data from Finngen
path = "path/to/finngen/summary/stats"
finngen_summary_stats = FinngenSummaryStats.from_source(session.spark, path)
```

## Creating a dataset from a pandas DataFrame

If none of our data sources fit your needs, you can create a `Dataset` object from your own data. To do so, you need to transform your data to fit the `Dataset` schema.

!!! info "The schema of a Dataset is defined in Spark format"

    The Dataset schemas can be found in the documentation of each Dataset. For example, the schema of the `SummaryStatistics` dataset can be found [here](../../python_api/datasets/summary_statistics.md).

You can also create a `Dataset` from a pandas DataFrame. This is useful when you want to create a `Dataset` from a small dataset that fits in memory.

```python
from gentropy.datasets.summary_statistics import SummaryStatistics
import pyspark.pandas as ps

# Load your transformed data into a pandas DataFrame
path = "path/to/your/data"
custom_summary_stats_pandas_df = pd.read_csv(path)

# Create a SummaryStatistics object specifying the data and schema
custom_summary_stats_df = ps.from_pandas(custom_summary_stats_pandas_df).to_spark()
custom_summary_stats = SummaryStatistics(
    _df=custom_summary_stats_df,
    _schema=SummaryStatistics.get_schema()
)
```
