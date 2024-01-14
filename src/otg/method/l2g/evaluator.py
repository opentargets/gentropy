"""Module that integrates Spark ML Evaluators with W&B for experiment tracking."""
from __future__ import annotations

import itertools
from typing import TYPE_CHECKING, Any, Dict

from pyspark import keyword_only
from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator,
    Evaluator,
    MulticlassClassificationEvaluator,
)
from pyspark.ml.param import Param, Params, TypeConverters
from wandb.sdk.wandb_run import Run

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


class WandbEvaluator(Evaluator):
    """Wrapper for pyspark Evaluators. It is expected that the user will provide an Evaluators, and this wrapper will log metrics from said evaluator to W&B."""

    spark_ml_evaluator: Param[Evaluator] = Param(
        Params._dummy(), "spark_ml_evaluator", "evaluator from pyspark.ml.evaluation"
    )

    wandb_run: Param[Run] = Param(
        Params._dummy(),
        "wandb_run",
        "wandb run.  Expects an already initialized run.  You should set this, or wandb_run_kwargs, NOT BOTH",
    )

    wandb_run_kwargs: Param[Any] = Param(
        Params._dummy(),
        "wandb_run_kwargs",
        "kwargs to be passed to wandb.init.  You should set this, or wandb_runId, NOT BOTH.  Setting this is useful when using with WandbCrossValdidator",
    )

    wandb_runId: Param[str] = Param(  # noqa: N815
        Params._dummy(),
        "wandb_runId",
        "wandb run id.  if not providing an intialized run to wandb_run, a run with id wandb_runId will be resumed",
    )

    wandb_project_name: Param[str] = Param(
        Params._dummy(),
        "wandb_project_name",
        "name of W&B project",
        typeConverter=TypeConverters.toString,
    )

    label_values: Param[list[str]] = Param(
        Params._dummy(),
        "label_values",
        "for classification and multiclass classification, this is a list of values the label can assume\nIf provided Multiclass or Multilabel evaluator without label_values, we'll figure it out from dataset passed through to evaluate.",
    )

    _input_kwargs: Dict[str, Any]

    @keyword_only
    def __init__(
        self: WandbEvaluator,
        label_values: list[str] | None = None,
        **kwargs: BinaryClassificationEvaluator
        | MulticlassClassificationEvaluator
        | Run,
    ) -> None:
        """Initialize a WandbEvaluator.

        Args:
            label_values (list[str] | None): List of label values.
            **kwargs (BinaryClassificationEvaluator | MulticlassClassificationEvaluator | Run): Keyword arguments.
        """
        if label_values is None:
            label_values = []
        super(Evaluator, self).__init__()

        self.metrics = {
            MulticlassClassificationEvaluator: [
                "f1",
                "accuracy",
                "weightedPrecision",
                "weightedRecall",
                "weightedTruePositiveRate",
                "weightedFalsePositiveRate",
                "weightedFMeasure",
                "truePositiveRateByLabel",
                "falsePositiveRateByLabel",
                "precisionByLabel",
                "recallByLabel",
                "fMeasureByLabel",
                "logLoss",
                "hammingLoss",
            ],
            BinaryClassificationEvaluator: ["areaUnderROC", "areaUnderPR"],
        }

        self._setDefault(label_values=[])
        kwargs = self._input_kwargs
        self._set(**kwargs)

    def setspark_ml_evaluator(self: WandbEvaluator, value: Evaluator) -> None:
        """Set the spark_ml_evaluator parameter.

        Args:
            value (Evaluator): Spark ML evaluator.
        """
        self._set(spark_ml_evaluator=value)

    def setlabel_values(self: WandbEvaluator, value: list[str]) -> None:
        """Set the label_values parameter.

        Args:
            value (list[str]): List of label values.
        """
        self._set(label_values=value)

    def getspark_ml_evaluator(self: WandbEvaluator) -> Evaluator:
        """Get the spark_ml_evaluator parameter.

        Returns:
            Evaluator: Spark ML evaluator.
        """
        return self.getOrDefault(self.spark_ml_evaluator)

    def getwandb_run(self: WandbEvaluator) -> Run:
        """Get the wandb_run parameter.

        Returns:
            Run: Wandb run object.
        """
        return self.getOrDefault(self.wandb_run)

    def getwandb_project_name(self: WandbEvaluator) -> Any:
        """Get the wandb_project_name parameter.

        Returns:
            Any: Name of the W&B project.
        """
        return self.getOrDefault(self.wandb_project_name)

    def getlabel_values(self: WandbEvaluator) -> list[str]:
        """Get the label_values parameter.

        Returns:
            list[str]: List of label values.
        """
        return self.getOrDefault(self.label_values)

    def _evaluate(self: WandbEvaluator, dataset: DataFrame) -> float:
        """Evaluate the model on the given dataset.

        Args:
            dataset (DataFrame): Dataset to evaluate the model on.

        Returns:
            float: Metric value.
        """
        dataset.persist()
        metric_values: list[tuple[str, Any]] = []
        label_values = self.getlabel_values()
        spark_ml_evaluator: BinaryClassificationEvaluator | MulticlassClassificationEvaluator = (
            self.getspark_ml_evaluator()  # type: ignore[assignment, unused-ignore]
        )
        run = self.getwandb_run()
        evaluator_type = type(spark_ml_evaluator)
        for metric in self.metrics[evaluator_type]:
            if "ByLabel" in metric and label_values == []:
                print(  # noqa: T201
                    "no label_values for the target have been provided and will be determined by the dataset.  This could take some time"
                )
                label_values = [
                    r[spark_ml_evaluator.getLabelCol()]
                    for r in dataset.select(spark_ml_evaluator.getLabelCol())
                    .distinct()
                    .collect()
                ]
                if isinstance(label_values[0], list):
                    merged = list(itertools.chain(*label_values))
                    label_values = list(dict.fromkeys(merged).keys())
                    self.setlabel_values(label_values)
            for label in label_values:
                out = spark_ml_evaluator.evaluate(
                    dataset,
                    {
                        spark_ml_evaluator.metricLabel: label,  # type: ignore[assignment, unused-ignore]
                        spark_ml_evaluator.metricName: metric,
                    },
                )
                metric_values.append((f"{metric}:{label}", out))
            out = spark_ml_evaluator.evaluate(
                dataset, {spark_ml_evaluator.metricName: metric}
            )
            metric_values.append((f"{metric}", out))
        run.log(dict(metric_values))
        config = [
            (f"{k.parent.split('_')[0]}.{k.name}", v)
            for k, v in spark_ml_evaluator.extractParamMap().items()
            if "metric" not in k.name
        ]
        run.config.update(dict(config))
        return_metric = spark_ml_evaluator.evaluate(dataset)
        dataset.unpersist()
        return return_metric
