---
title: Applying methods
---

The available methods implement well established algorithms that transform and analyse data. Methods usually take as input predefined `Dataset`(s) and produce one or several `Dataset`(s) as output. This section explains how to apply methods to your data.

The full list of available methods can be found in the Python API [documentation](../../python_api/methods/_methods.md).

## Apply a class method

Some methods are implemented as class methods. For example, the `finemap` method is a class method of the [`PICS`](../../python_api/methods/pics.md) class. This method performs fine-mapping using the PICS algorithm. These methods usually take as input one or several `Dataset`(s) and produce one or several `Dataset`(s) as output.

```python
--8<-- "src_snippets/howto/python_api/c_applying_methods.py:apply_class_method_pics"
```

## Apply a `Dataset` instance method

Some methods are implemented as instance methods of the `Dataset` class. For example, the `window_based_clumping` method is an instance method of the `SummaryStatistics` class. This method performs window-based clumping on summary statistics.

```python
--8<-- "src_snippets/howto/python_api/c_applying_methods.py:apply_instance_method"
```

!!! info "The `window_based_clumping` method is also available as a class method"

    The `window_based_clumping` method is also available as a class method of the `WindowBasedClumping` class. This method performs window-based clumping on summary statistics.

    ```python
    # Perform window-based clumping on summary statistics
    --8<-- "src_snippets/howto/python_api/c_applying_methods.py:apply_class_method_clumping"
    ```

## What's next?

Up next, we'll show you how to inspect your data to ensure its integrity and the success of your transformations.
