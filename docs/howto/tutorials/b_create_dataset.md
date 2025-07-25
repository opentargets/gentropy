---
title: Create a dataset
---

## Datasets

**Gentropy Datasets** are the most basic concept that allows to represent various abstract data modalities: _Variant, Gene, Locus_, etc.

The full list of `Dataset`s is available in the Python API [documentation](../../python_api/datasets/_datasets.md).

!!! info "Any instance of Dataset will have 2 common attributes"

    - **df**: the Spark DataFrame that contains the data
    - **schema**: the definition of the data structure in Spark format

### Dataset implementation - pyspark DataFrame

Datasets are implemented as Classes that are composed of the **PySpark DataFrames**, this means that the `Dataset` has a `df` attribute that references a dataframe with the specific schema.

### Dataset schemas - contract with the user

Each dataset specifies the **contract that has to be met by the data provided by the package user** in order to run methods implemented in Gentropy.

The dataset contract is implemented as a pyspark DataFrame schema under `schema` attribute and includes the table field names, types and allows for specifying if a field is required or optional to construct the dataset. All dataset schemas can be found in the corresponding documentation pages under the [Dataset API](../../python_api/datasets/_datasets.md).

## Dataset initialization

In this section you'll learn the different ways of how to create a `Dataset` instances.

### Initializing datasets from parquet files

Each dataset has a method to read the `parquet` files into the `Dataset` instance with schema validation. This is implemented in the `Dataset.from_parquet` abstract method.

```python
--8<-- "src_snippets/howto/python_api/b_create_dataset.py:create_from_parquet_import"
path = "path/to/summary/stats"
--8<-- "src_snippets/howto/python_api/b_create_dataset.py:create_from_parquet"
```

!!! info "Parquet files"

    Parquet is a columnar storage format that is widely used in the Spark ecosystem. It is the recommended format for storing large datasets. For more information about parquet, please visit [https://parquet.apache.org/](https://parquet.apache.org/).

!!! info "Reading multiple files"

    If you have multiple parquet files, you can pass a

    - directory path like `path/to/summary/stats` - reading all parquet files from `stats` directory.
    - glob pattern like `path/to/summary/stats/*h.parquet` - reading all files that ends with `.parquet` from `stats directory.

    to the `from_parquet` method. The method will read all the parquet files in the directory and return a `Dataset` instance.

### Initializing datasets from pyspark DataFrames

Once one already has a pyspark DataFrame, it can be converted to a dataset using the default `Dataset` constructor. The constructor also validates the schema of the provided DataFrame against the dataset schema.

## Initializing datasets from a data source

Alternatively, `Dataset`s can be created using a [data source](../../python_api/datasources/_datasources.md) harmonisation method. For example, to create a `SummaryStatistics` object from Finngen's raw summary statistics, you can use the [`FinnGen`](../../python_api/datasources/finngen/summary_stats.md) data source.

```python
--8<-- "src_snippets/howto/python_api/b_create_dataset.py:create_from_source_import"
path = "path/to/finngen/summary/stats"
--8<-- "src_snippets/howto/python_api/b_create_dataset.py:create_from_source"
```

## Initializing datasets from a pandas DataFrame

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
