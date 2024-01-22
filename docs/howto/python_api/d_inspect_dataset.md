---
title: Inspect a dataset
---

We have seen how to create and transform a `Dataset` instance. This section guides you through inspecting your data to ensure its integrity and the success of your transformations.

## Inspect data in a `Dataset`

The `df` attribute of a Dataset instance is key to interacting with and inspecting the stored data.

!!! info "By accessing the df attribute, you can apply any method that you would typically use on a PySpark DataFrame. See the [PySpark documentation](https://spark.apache.org/docs/3.1.1/api/python/reference/pyspark.sql.html#dataframe-apis) for more information."

### View data samples

```python
# Inspect the first 10 rows of the data
summary_stats.df.show(10)
```

This method displays the first 10 rows of your dataset, giving you a snapshot of your data's structure and content.

### Understand the schema

```python
# Get the Spark schema of any `Dataset` as a `StructType` object
summary_stats.get_schema()

# Print the schema of the data
summary_stats.df.printSchema()
```

## Write a `Dataset` to disk

```python
# Write the data to disk in parquet format
summary_stats.df.write.parquet("path/to/summary/stats")

# Write the data to disk in csv format
summary_stats.df.write.csv("path/to/summary/stats")
```

Consider the format's compatibility with your tools, and the partitioning strategy for large datasets to optimize performance.
