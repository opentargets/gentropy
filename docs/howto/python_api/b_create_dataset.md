---
title: Create a dataset
---

Gentropy provides a collection of `Dataset`s that encapsulate key concepts in the field of genetics. For example, to represent summary statistics, you'll use the [`SummaryStatistics`](../../python_api/datasets/summary_statistics.md) class. This datatype comes with a set of useful operations to disentangle the genetic architecture of a trait or disease.

The full list of `Dataset`s is available in the Python API [documentation](../../python_api/datasets/_datasets.md).

!!! info "Any instance of Dataset will have 2 common attributes"

    - **df**: the Spark DataFrame that contains the data
    - **schema**: the definition of the data structure in Spark format

In this section you'll learn the different ways of how to create a `Dataset` instances.

## Creating a dataset from parquet

All the `Dataset`s have a `from_parquet` method that allows you to create any `Dataset` instance from a parquet file or directory.

```python
--8<-- "src_snippets/howto/python_api/b_create_dataset.py:create_from_parquet_import"
path = "path/to/summary/stats"
--8<-- "src_snippets/howto/python_api/b_create_dataset.py:create_from_parquet"
```

!!! info "Parquet files"

    Parquet is a columnar storage format that is widely used in the Spark ecosystem. It is the recommended format for storing large datasets. For more information about parquet, please visit [https://parquet.apache.org/](https://parquet.apache.org/).

## Creating a dataset from a data source

Alternatively, `Dataset`s can be created using a [data source](../../python_api/datasources/_datasources.md) harmonisation method. For example, to create a `SummaryStatistics` object from Finngen's raw summary statistics, you can use the [`FinnGen`](../../python_api/datasources/finngen/summary_stats.md) data source.

```python
--8<-- "src_snippets/howto/python_api/b_create_dataset.py:create_from_source_import"
path = "path/to/finngen/summary/stats"
--8<-- "src_snippets/howto/python_api/b_create_dataset.py:create_from_source"
```

## Creating a dataset from a pandas DataFrame

If none of our data sources fit your needs, you can create a `Dataset` object from your own data. To do so, you need to transform your data to fit the `Dataset` schema.

!!! info "The schema of a Dataset is defined in Spark format"

    The Dataset schemas can be found in the documentation of each Dataset. For example, the schema of the `SummaryStatistics` dataset can be found [here](../../python_api/datasets/summary_statistics.md).

You can also create a `Dataset` from a pandas DataFrame. This is useful when you want to create a `Dataset` from a small dataset that fits in memory.

```python
--8<-- "src_snippets/howto/python_api/b_create_dataset.py:create_from_pandas_import"

# Load your transformed data into a pandas DataFrame
path = "path/to/your/data"
custom_summary_stats_pandas_df = pd.read_csv(path)
--8<-- "src_snippets/howto/python_api/b_create_dataset.py:create_from_pandas"
```

## What's next?

In the next section, we will explore how to apply well-established algorithms that transform and analyse genetic data within the Gentropy framework.
