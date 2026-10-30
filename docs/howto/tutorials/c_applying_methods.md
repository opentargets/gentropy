---
title: Applying methods
---

The available methods implement well established algorithms that transform and analyse data. Methods usually take as input predefined `Dataset`(s) and produce one or several `Dataset`(s) as output. This section explains how to apply methods to your data.

The full list of available methods can be found in the Python API [documentation](../../python_api/methods/_methods.md).

## Apply a class method

Some methods are implemented as classes. For example, the `finemap` method of the [`PICS`](../../python_api/methods/pics.md) class performs fine-mapping using the PICS algorithm. These methods usually take as input one or several `Dataset`(s) and produce one or several `Dataset`(s) as output: `finemap` takes LD annotated study loci and the study index, and returns the credible sets and their variants.

```python
--8<-- "src_snippets/howto/python_api/c_applying_methods.py:apply_class_method_pics"
```

## Apply a `Dataset` instance method

Some methods are implemented as instance methods of the `Dataset` class. For example, the `pvalue_filter` method is an instance method of the `SummaryStatistics` class. It keeps the associations at least as significant as a p-value threshold.

```python
--8<-- "src_snippets/howto/python_api/c_applying_methods.py:apply_instance_method"
```

## Apply a method class

Methods with parameters are classes: create the method once with its parameters, then apply it to datasets. For example, [`WindowClumping`](../../python_api/methods/clumping.md) clumps summary statistics into study loci.

```python
--8<-- "src_snippets/howto/python_api/c_applying_methods.py:apply_class_method_clumping"
```

## What's next?

Up next, we'll show you how to inspect your data to ensure its integrity and the success of your transformations.
