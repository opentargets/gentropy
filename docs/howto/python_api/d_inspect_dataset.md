---
title: Inspect a dataset
---

We have seen how to create and transform a `Dataset` instance. This section guides you through inspecting your data to ensure its integrity and the success of your transformations.

## Inspect data in a `Dataset`

The `df` attribute of a Dataset instance is key to interacting with and inspecting the stored data.

!!! info "By accessing the df attribute, you can apply any method that you would typically use on a PySpark DataFrame. See the [PySpark documentation](https://spark.apache.org/docs/3.1.1/api/python/reference/pyspark.sql.html#dataframe-apis) for more information."

### View data samples

```python
--8<-- "src_snippets/howto/python_api/d_inspect_dataset.py:print_dataframe"
```

This method displays the first 10 rows of your dataset, giving you a snapshot of your data's structure and content.

### Understand the schema

```python
--8<-- "src_snippets/howto/python_api/d_inspect_dataset.py:get_dataset_schema"

--8<-- "src_snippets/howto/python_api/d_inspect_dataset.py:print_dataframe"
```

## Write a `Dataset` to disk

```python
--8<-- "src_snippets/howto/python_api/d_inspect_dataset.py:write_parquet"

--8<-- "src_snippets/howto/python_api/d_inspect_dataset.py:write_csv"
```

Consider the format's compatibility with your tools, and the partitioning strategy for large datasets to optimize performance.
