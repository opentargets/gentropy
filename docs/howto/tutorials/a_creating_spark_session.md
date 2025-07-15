---
title: Creating a Spark Session
---

In this section, we'll guide you through creating a Spark session using Gentropy's Session class. Gentropy uses _Apache PySpark_ as the underlying framework for distributed computing. The Session class provides a convenient way to initialize a Spark session with pre-configured settings.

## Creating a Default Session

To begin your journey with Gentropy, start by creating a default Spark session. This is the simplest way to initialize your environment.

```python
--8<-- "src_snippets/howto/python_api/a_creating_spark_session.py:default_session"
```

The above code snippet sets up a default Spark session with pre-configured settings. This is ideal for getting started quickly without needing to tweak any configurations.

## Customizing Your Spark Session

Gentropy allows you to customize the Spark session to suit your specific needs. You can modify various parameters such as memory allocation, number of executors, and more. This flexibility is particularly useful for optimizing performance in steps that are more computationally intensive.

### Example: Increasing Driver Memory

If you require more memory for the Spark driver, you can easily adjust this setting:

```python
--8<-- "src_snippets/howto/python_api/a_creating_spark_session.py:custom_session"
```

This code snippet demonstrates how to increase the memory allocated to the Spark driver to 16 gigabytes. You can customize other Spark settings similarly, according to your project's requirements.

## What's next?

Now that you've created a Spark session, you're ready to start using Gentropy. In the next section, we'll show you how to process a large dataset using Gentropy's powerful _SummaryStatistics_ datatype.
