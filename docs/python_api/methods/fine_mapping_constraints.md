---
title: Fine-Mapping Constraints
---

**Fine-Mapping Constraints Overview:**

Before a study can be routed to a fine-mapping method (e.g. MultiSuSiE), it must satisfy a
set of eligibility constraints derived from the study index (study type, summary statistics
availability, mapped trait, quality control flags, analysis flags, and major ancestry).

Each constraint is a small, independently testable unit (`MethodConstraint`) that annotates
a `StudyIndex` with a boolean verdict. A `ConstraintSet` (e.g. `MultiSuSiEConstraintSet`)
composes a list of constraints, resolves them against a `StudyIndex`, and produces a
[Fine-Mapping Plan](../datasets/fine_mapping.md) grouping eligible studies into individual
or multi-ancestry fine-mapping runs.

::: gentropy.method.fine_mapping.constraint.MethodConstraint

::: gentropy.method.fine_mapping.constraint.IsAllowedStudyType

::: gentropy.method.fine_mapping.constraint.HasSumstats

::: gentropy.method.fine_mapping.constraint.HasMappedTrait

::: gentropy.method.fine_mapping.constraint.PassSumstatQC

::: gentropy.method.fine_mapping.constraint.HasAllowedAnalysisFlags

::: gentropy.method.fine_mapping.constraint.HasAllowedMajorAncestry

::: gentropy.method.fine_mapping.constraint_set.ConstraintSet

::: gentropy.method.fine_mapping.constraint_set.MultiSuSiEConstraintSet
