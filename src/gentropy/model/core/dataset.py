"""The Dataset base class: a DataFrame known to carry a set of traits.

Every gentropy dataset is a subclass of ``Dataset`` that states two things:

* its traits, as the type argument: ``class CredibleSet(Dataset[HasStudyId |
  HasRegion])``. A union of traits reads as "has all of these".
* its columns, as class attributes declared with ``optional``, ``required`` or
  ``implements`` (see the ``schema`` module). ``Dataset`` builds ``schema``
  from them, in declaration order.

The traits are what the type checker sees. A method that needs a study id and a
region asks for ``Dataset[HasStudyId | HasRegion]``, and it accepts every dataset
that declares at least those two traits. The schema is what Spark sees: every
column, mandatory or not, in the order it is written. On an instance, each column
attribute is its DataFrame column, so ``loci.locus_start`` is ``loci.df["locusStart"]``.

Why the type argument is contravariant
--------------------------------------

``T_contra`` is declared contravariant, so ``Dataset[A | B]`` is a subtype of
``Dataset[A]``. A dataset with more traits can be passed wherever fewer are
required, which is the subtyping we want. The declaration also makes mypy
reject any method on ``Dataset`` that returns ``T_contra``, because that would
make the parameter invariant and quietly switch the subtyping off for every
dataset. Python 3.11 is still supported, so the class uses ``TypeVar`` and
``Generic`` rather than the ``class Dataset[T]`` syntax of Python 3.12.

What is checked when a dataset class is defined
-----------------------------------------------

``Dataset`` checks each subclass once, at import time, and raises
``DatasetError`` if any rule is broken:

* The class names its traits, as in ``Dataset[A | B]``. A dataset that has none
  says so with ``Dataset[Trait]``; it can then only be passed to methods that
  ask for it by name, and its own ``required`` columns are still checked.
* The class declares at least one column.
* Every field of every declared trait is implemented by a column, through
  ``implements``. A column with a matching name does not count without that
  link. Without this a method could accept the dataset through its type and then
  find a column missing, or read a column that means something else.
* No column implements a field of a trait the dataset does not declare, since the
  type checker would never see that trait.
* Column attributes are snake_case and do not reuse a name ``Dataset`` already
  has (``df``, ``schema``, ``traits``, ``print_schema``, ``pipe``).
* A trait ``Field`` is never assigned directly in a dataset class. Doing so
  would rebind the trait's own field; ``implements`` makes the column instead.

Every problem with the trait links is reported in one message.

A dataset class lists its traits directly. Inheriting from another named
dataset is rejected: the subclass would inherit the parent's type argument, so
the type checker could not see the traits it adds.

Sharing columns between datasets
--------------------------------

Two datasets that differ by a few columns, such as a study locus before and
after its ``locus`` is collected, declare the shared columns once in a plain
mixin class and list it before ``Dataset[...]``. The mixin's columns come
first in the schema, in the order of the bases, and are checked like the
class's own. Each dataset still names all of its traits, so the type checker
sees exactly what it has; a ``TypeAlias`` for the shared union keeps that to
one line. A column declared both in a mixin and in the class is rejected.

Building a dataset from data
----------------------------

``Loci(df)`` checks ``df.schema`` against ``Loci.schema`` and keeps ``df``
exactly as it is. It never reorders columns or adds missing ones; column order
is applied when the dataset is written. The check uses the comparison ported
from the legacy package, and a mismatch raises ``SchemaValidationError`` with
every problem listed:

* a mandatory column that is missing is an error, because methods rely on it.
  Mandatory columns are found by the marker ``implements`` and ``required``
  put on them;
* a column the schema does not list is an error, which also catches typos;
* a column with a different type is an error;
* a missing optional column is accepted and means the same as a column of
  nulls;
* inside a column, every field of a struct is mandatory, including the structs
  of an array, as methods read nested fields without checking for them;
* nullable flags are not compared. Every column in a dataset schema is
  nullable anyway, and the flag on a DataFrame says nothing about its values.

``Dataset`` itself holds no data, since it has no schema to check against.

Documenting a dataset
---------------------

``X.print_schema()`` prints the layout as a tree, with every top-level column
labelled mandatory (naming the traits that require it) or optional. Spark's
``printSchema`` cannot show that, so dataset docstrings use ``print_schema`` in a
doctest, which also keeps the documented layout in step with the code.

Example:
    >>> from pyspark.sql import types as t
    >>> from gentropy.model.core.schema import implements, optional
    >>> from gentropy.model.core.trait import Field, Trait
    >>> class HasStudyId(Trait):
    ...     study_id = Field(t.StringType())
    >>> class HasRegion(Trait):
    ...     chromosome = Field(t.StringType())
    ...     locus_start = Field(t.IntegerType())
    ...     locus_end = Field(t.IntegerType())
    >>> class Loci(Dataset[HasStudyId | HasRegion]):
    ...     study_id = implements(HasStudyId.study_id)
    ...     chromosome = implements(HasRegion.chromosome)
    ...     locus_start = implements(HasRegion.locus_start)
    ...     locus_end = implements(HasRegion.locus_end)
    ...     confidence = optional(t.ByteType())
    >>> sorted(trait.__name__ for trait in Loci.traits)
    ['HasRegion', 'HasStudyId']
    >>> Loci.schema.fieldNames()
    ['studyId', 'chromosome', 'locusStart', 'locusEnd', 'confidence']

    Building it from data. The optional ``confidence`` column may be absent:

    >>> df = spark.createDataFrame(
    ...     [("GCST1", "1", 100, 200)],
    ...     "studyId string, chromosome string, locusStart int, locusEnd int",
    ... )
    >>> loci = Loci(df)
    >>> loci.df is df
    True
    >>> loci.df.select(loci.locus_start).first()
    Row(locusStart=100)

    A DataFrame with a column the schema does not list is rejected:

    >>> Loci(df.withColumn("pValue", df.locusStart * 1.0))
    Traceback (most recent call last):
    ...
    gentropy.model.core.validation.SchemaValidationError: DataFrame does not match the Loci schema
    Errors:
      unexpected_columns: pValue

    A dataset that leaves trait fields without a column is rejected, with every gap
    listed. A column of the right name does not count unless it is linked:

    >>> class Partial(Dataset[HasStudyId | HasRegion]):
    ...     study_id = implements(HasStudyId.study_id)
    ...     chromosome = implements(HasRegion.chromosome)
    ...     locus_start = optional(t.IntegerType())
    Traceback (most recent call last):
    ...
    gentropy.model.core.dataset.DatasetError: Partial: HasRegion.locus_start is not implemented; column locusStart exists but is not linked, declare it with implements(...); HasRegion.locus_end is not implemented

    Sharing the region columns between two datasets through a mixin:

    >>> class RegionColumns:
    ...     chromosome = implements(HasRegion.chromosome)
    ...     locus_start = implements(HasRegion.locus_start)
    ...     locus_end = implements(HasRegion.locus_end)
    >>> class Regions(RegionColumns, Dataset[HasRegion]):
    ...     pass
    >>> class StudyRegions(RegionColumns, Dataset[HasRegion | HasStudyId]):
    ...     study_id = implements(HasStudyId.study_id)
    >>> StudyRegions.schema.fieldNames()
    ['chromosome', 'locusStart', 'locusEnd', 'studyId']
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from types import UnionType
from typing import (
    Any,
    ClassVar,
    Concatenate,
    Generic,
    ParamSpec,
    Self,
    TypeVar,
    Union,
    cast,
    get_args,
    get_origin,
)

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as f
from pyspark.sql import types as t

from gentropy.model.core.schema import DatasetColumn
from gentropy.model.core.trait import (
    SNAKE_CASE,
    TRAITS,
    Field,
    Trait,
    is_mandatory,
)
from gentropy.model.core.validation import SchemaValidationError, compare_struct_schemas

T_contra = TypeVar("T_contra", bound=Trait, contravariant=True)
P = ParamSpec("P")
R = TypeVar("R")


class DatasetError(TypeError):
    """A dataset class is declared incorrectly.

    Like ``TraitError`` and ``SchemaError``, it is raised while classes are being
    defined, so it points at the class declaration and never at data.
    """


def _flatten(item: Any) -> list[Any]:
    """Turn ``A | B`` or ``A`` into ``[A, B]`` or ``[A]``.

    Args:
        item (Any): The type argument of ``Dataset[...]``.

    Returns:
        list[Any]: The traits it names.
    """
    if get_origin(item) in (Union, UnionType):
        return [leaf for arg in get_args(item) for leaf in _flatten(arg)]
    return [item]


def _contains_struct(dtype: t.DataType) -> bool:
    """Whether ``dtype`` is a struct, or an array that holds structs at any depth.

    Args:
        dtype (t.DataType): Type to inspect.

    Returns:
        bool: True if the type is drawn as a tree.
    """
    if isinstance(dtype, t.ArrayType):
        return _contains_struct(dtype.elementType)
    return isinstance(dtype, t.StructType)


def _label(dtype: t.DataType) -> str:
    """Type label of a schema tree line: a bare name when children follow.

    Args:
        dtype (t.DataType): Type to label.

    Returns:
        str: ``struct`` or ``array`` for types drawn as a tree, else the full type.
    """
    return dtype.typeName() if _contains_struct(dtype) else dtype.simpleString()


def _nested_lines(dtype: t.DataType, indent: str) -> list[str]:
    """Tree lines for the children of ``dtype``, in Spark's ``printSchema`` style.

    Args:
        dtype (t.DataType): Type whose children to draw.
        indent (str): Prefix that places the lines under their parent.

    Returns:
        list[str]: One line per nested field or array element.
    """
    if not _contains_struct(dtype):
        return []
    if isinstance(dtype, t.ArrayType):
        element = dtype.elementType
        return [f"{indent} |-- element: {_label(element)}"] + _nested_lines(
            element, indent + " |   "
        )
    lines = []
    for field in cast(t.StructType, dtype).fields:
        lines.append(f"{indent} |-- {field.name}: {_label(field.dataType)}")
        lines += _nested_lines(field.dataType, indent + " |   ")
    return lines


def _link_problems(
    name: str, columns: list[DatasetColumn], traits: frozenset[type[Trait]]
) -> list[str]:
    """List links to undeclared traits and trait fields left without a column.

    Args:
        name (str): Dataset class name, for the messages.
        columns (list[DatasetColumn]): The dataset's columns.
        traits (frozenset[type[Trait]]): The traits the dataset declares.

    Returns:
        list[str]: One message per problem; empty when every field is linked.
    """
    problems = []
    linked = set()
    for column in columns:
        for field in column.links:
            linked.add(field.key)
            if field.owner not in traits:
                owner = field.owner.__name__ if field.owner else "?"
                problems.append(
                    f"{column.name} links {field.key} but {name} does not declare "
                    f"{owner}"
                )
    names = {column.name for column in columns}
    for trait in sorted(traits, key=lambda trait: trait.__name__):
        for field in trait.fields:
            if field.key in linked:
                continue
            problem = f"{field.key} is not implemented"
            if field.name in names:
                problem += (
                    f"; column {field.name} exists but is not linked, "
                    "declare it with implements(...)"
                )
            problems.append(problem)
    return problems


def _class_attributes(cls: type) -> list[tuple[str, Any]]:
    """Attributes of ``cls`` and of its column mixins, mixins first in base order.

    A mixin is any base that is not a dataset, ``Generic`` or ``object``.

    Args:
        cls (type): The dataset class being defined.

    Returns:
        list[tuple[str, Any]]: (attribute, value) pairs in declaration order.

    Raises:
        DatasetError: If a column is declared both in a mixin and in the class.
    """
    mixins = [
        base
        for base in reversed(cls.__mro__[1:])
        if not issubclass(base, Dataset) and base not in (object, Generic)
    ]
    attributes: list[tuple[str, Any]] = []
    columns: set[str] = set()
    for owner in [*mixins, cls]:
        for attribute, value in vars(owner).items():
            if isinstance(value, DatasetColumn):
                if attribute in columns:
                    raise DatasetError(
                        f"{cls.__name__}.{attribute}: the column is declared twice, "
                        "in a mixin and again in the class"
                    )
                columns.add(attribute)
            attributes.append((attribute, value))
    return attributes


class Dataset(Generic[T_contra]):
    """Base class of every dataset.

    Subclass it as ``class X(Dataset[A | B])`` and declare its columns as class
    attributes with ``optional`` and ``implements``. The module docstring lists the
    checks that run when the subclass is defined.

    Attributes:
        traits (frozenset[type[Trait]]): The traits named in ``Dataset[...]``.
        schema (t.StructType): The column layout, built from the column attributes.

    Examples:
        >>> from pyspark.sql import types as t
        >>> from gentropy.model.core.schema import implements, optional
        >>> from gentropy.model.core.trait import Field, Trait
        >>> class HasStudyId(Trait):
        ...     study_id = Field(t.StringType())

        A dataset must name its traits:

        >>> class Bare(Dataset):
        ...     note = optional(t.StringType())
        Traceback (most recent call last):
        ...
        gentropy.model.core.dataset.DatasetError: Bare: declare the traits the dataset implements, e.g. `class X(Dataset[A | B])`

        A dataset must declare its columns:

        >>> class NoLayout(Dataset[HasStudyId]):
        ...     pass
        Traceback (most recent call last):
        ...
        gentropy.model.core.dataset.DatasetError: NoLayout: declare at least one column with `optional(...)`, `required(...)` or `implements(...)`

        A trait field is stored through ``implements``, never assigned directly:

        >>> class Direct(Dataset[HasStudyId]):
        ...     study_id = HasStudyId.study_id
        Traceback (most recent call last):
        ...
        gentropy.model.core.dataset.DatasetError: Direct.study_id: a trait field cannot be assigned directly; declare `study_id = implements(...)`
    """

    traits: ClassVar[frozenset[type[Trait]]] = frozenset()
    schema: ClassVar[t.StructType]

    def __init__(self, df: DataFrame) -> None:
        """Wrap ``df`` as this dataset after checking it against ``schema``.

        Args:
            df (DataFrame): The data.

        Raises:
            DatasetError: If called on ``Dataset`` itself instead of a named dataset.
            SchemaValidationError: If ``df`` does not match the dataset's schema.
        """
        if type(self) is Dataset:
            raise DatasetError(
                "Dataset holds no data itself; build a named dataset such as "
                "`class X(Dataset[A | B])` instead"
            )
        issues = compare_struct_schemas(df.schema, self.schema)
        # Every column is nullable, so mandatory columns are found by their marker.
        present = set(df.columns)
        if missing := [
            field.name
            for field in self.schema.fields
            if is_mandatory(field) and field.name not in present
        ]:
            issues["missing_mandatory_columns"] += missing
        if issues:
            raise SchemaValidationError(
                f"DataFrame does not match the {type(self).__name__} schema", issues
            )
        self.df = df

    @staticmethod
    def generate_identifier(uniqueness_defining_columns: Sequence[str]) -> Column:
        """Hashes the provided columns to generate a unique identifier.

        Args:
            uniqueness_defining_columns (Sequence[str]): list of columns defining uniqueness

        Returns:
            Column: column with a unique identifier
        """
        hashable_columns = [
            f.when(f.col(column).cast("string").isNull(), f.lit("None")).otherwise(
                f.col(column).cast("string")
            )
            for column in uniqueness_defining_columns
        ]
        return f.md5(f.concat(*hashable_columns))

    def pipe(
        self,
        func: Callable[Concatenate[Self, P], R],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> R:
        """Call ``func(self, *args, **kwargs)``, so operations chain like methods.

        Operations that work on any dataset with a given trait are plain
        functions, such as ``pvalue_filter(dataset: D, pvalue) -> D`` with ``D``
        bound to ``Dataset[Association]``. ``pipe`` lets them read left to right:
        ``ss.pipe(pvalue_filter, 5e-8).pipe(...)``. The type checker sees the
        concrete dataset, so it checks that the dataset has the traits ``func``
        asks for, checks the extra arguments, and keeps the return type, which is
        whatever ``func`` returns.

        Args:
            func (Callable[Concatenate[Self, P], R]): The operation; it takes the
                dataset first.
            *args (P.args): Further positional arguments for ``func``.
            **kwargs (P.kwargs): Keyword arguments for ``func``.

        Returns:
            R: What ``func`` returns.

        Examples:
            >>> from pyspark.sql import types as t
            >>> from gentropy.model.core.schema import implements
            >>> from gentropy.model.core.trait import Field, Trait
            >>> class HasStudyId(Trait):
            ...     study_id = Field(t.StringType())
            >>> class Studies(Dataset[HasStudyId]):
            ...     study_id = implements(HasStudyId.study_id)
            >>> def count_studies(dataset: Dataset[HasStudyId], prefix: str) -> int:
            ...     study_id = HasStudyId.of(dataset).study_id
            ...     return dataset.df.filter(study_id.startswith(prefix)).count()
            >>> studies = Studies(spark.createDataFrame([("GCST1",), ("QTL1",)], "studyId string"))
            >>> studies.pipe(count_studies, "GCST")
            1
        """
        return func(self, *args, **kwargs)

    @classmethod
    def print_schema(cls) -> None:
        """Print the column layout, marking mandatory columns and their traits.

        Spark's ``printSchema`` cannot show which columns are mandatory, since that
        lives in field metadata. This prints the same tree with each top-level
        column labelled ``mandatory`` (with the trait fields it implements) or
        ``optional``. It needs no data and no Spark session, so dataset docstrings
        use it to document their layout.

        Examples:
            >>> from pyspark.sql import types as t
            >>> from gentropy.model.core.schema import implements, optional
            >>> from gentropy.model.core.trait import Field, Trait
            >>> class HasStudyId(Trait):
            ...     study_id = Field(t.StringType())
            >>> class Studies(Dataset[HasStudyId]):
            ...     study_id = implements(HasStudyId.study_id)
            ...     trait_from_source = optional(t.StringType())
            >>> Studies.print_schema()
            Studies
             |-- studyId: string (mandatory: HasStudyId.study_id)
             |-- traitFromSource: string (optional)
        """
        lines = [cls.__name__]
        for column in cls.schema.fields:
            traits = column.metadata.get(TRAITS)
            kind = (
                ("mandatory: " + ", ".join(traits) if traits else "mandatory")
                if is_mandatory(column)
                else "optional"
            )
            lines.append(f" |-- {column.name}: {_label(column.dataType)} ({kind})")
            lines += _nested_lines(column.dataType, " |   ")
        print("\n".join(lines))  # noqa: T201 (printing is this method's purpose)

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """Record the traits a named dataset declares.

        Args:
            **kwargs (Any): Passed on to ``Generic.__init_subclass__``.

        Raises:
            DatasetError: If the dataset does not declare its traits, has no
                schema, or its schema does not contain every trait column exactly
                as the trait declares it.
        """
        super().__init_subclass__(**kwargs)
        declared = [
            base
            for base in getattr(cls, "__orig_bases__", ())
            if get_origin(base) is Dataset
        ]
        if not declared:
            raise DatasetError(
                f"{cls.__name__}: declare the traits the dataset implements, "
                "e.g. `class X(Dataset[A | B])`"
            )
        if named := [
            base.__name__
            for base in cls.__bases__
            if issubclass(base, Dataset) and base is not Dataset
        ]:
            raise DatasetError(
                f"{cls.__name__}: cannot inherit from the named dataset "
                f"{', '.join(named)}; declare shared columns in a mixin class "
                "and list it before `Dataset[...]`"
            )
        cls.traits = frozenset(_flatten(get_args(declared[0])[0]))
        # ``df`` is set per instance and ``schema`` is only annotated on the base.
        reserved = set(dir(Dataset)) | {"df", "schema"}
        columns = []
        for attribute, value in _class_attributes(cls):
            if isinstance(value, Field):
                raise DatasetError(
                    f"{cls.__name__}.{attribute}: a trait field cannot be assigned "
                    f"directly; declare `{attribute} = implements(...)`"
                )
            if not isinstance(value, DatasetColumn):
                continue
            if attribute in reserved:
                raise DatasetError(
                    f"{cls.__name__}.{attribute}: the name is reserved by Dataset"
                )
            if not SNAKE_CASE.fullmatch(attribute):
                raise DatasetError(
                    f"{cls.__name__}.{attribute}: column attributes must be "
                    "snake_case so they map to a camelCase column name"
                )
            columns.append(value)
        if not columns:
            raise DatasetError(
                f"{cls.__name__}: declare at least one column with "
                "`optional(...)`, `required(...)` or `implements(...)`"
            )
        if problems := _link_problems(cls.__name__, columns, cls.traits):
            raise DatasetError(f"{cls.__name__}: " + "; ".join(problems))
        cls.schema = t.StructType([column.struct_field for column in columns])
