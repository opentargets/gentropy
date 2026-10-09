"""Traits: named groups of mandatory columns.

A trait says "a dataset that has me has these columns".

Methods can therefore ask for generic datasets that satisfy shared trait instead of
concrete dataset classes.
Example is the window based clumping, that requires position, variant id and pvalue to
do the clumping. This method can be applied on any dataset that contains this subset of fields.

How the pieces fit together
---------------------------

The dataset model has three layers:

1. ``Field`` and ``Trait`` (this module). A trait is a class whose ``Field``
   attributes name its columns, and it holds nothing else.
2. ``optional`` and ``implements`` (``schema`` module). They declare a
   dataset's columns as class attributes: an optional column, or a mandatory
   column that implements one or more trait fields.
3. ``Dataset[T]`` - The base class of every dataset. ``T`` is a union of
   the traits the dataset implements, and the class attributes are its columns,
   in order::

       class CredibleSet(Dataset[HasStudyId | HasLeadVariantId]):
           study_id = implements(HasStudyId.study_id)
           lead_variant_id = implements(HasLeadVariantId.lead_variant_id)
           confidence = optional(t.ByteType())


       def lead_variants(ds: Dataset[HasLeadVariantId]) -> DataFrame:
           return ds.df.select(HasLeadVariantId.of(ds).lead_variant_id)


       lead_variants(credible_set)  # accepted: CredibleSet implements HasLeadVariantId

A union of traits reads as "has all of these". The type parameter of ``Dataset``
is contravariant, so ``Dataset[A | B]`` is accepted wherever ``Dataset[A]`` or
``Dataset[B]`` is required. A method that needs two traits asks for
``Dataset[A | B]``, and no combined trait has to be declared for it.

Rules for writing a trait
-------------------------

``Trait`` checks each of these when a subclass is defined and raises
``TraitError`` on the first failed check. A mistake fails at import time.

* Declare at least one ``Field``. A trait without columns would be satisfied by
  every dataset, so it could not constrain anything.
* Name each field in snake_case. The column name is the camelCase form of the
  attribute, so ``p_value_mantissa`` declares the column ``pValueMantissa``.
  Leading, trailing and doubled underscores are rejected, and so are camelCase
  attributes, because only strict snake_case converts unambiguously.
* Put only mandatory columns in a trait. A trait column must exist in every
  dataset that implements the trait, but its values may still be null. Like
  every column in the model it is nullable, as Spark makes every column read
  from parquet; what marks it mandatory is the ``mandatory`` entry in its
  ``StructField`` metadata, which ``implements`` adds. A column that a dataset
  may omit is not a trait column. The dataset declares it with ``optional``.
* Subclass ``Trait`` directly and nothing else. Inheriting from another trait
  would reverse the subtyping: with a contravariant ``Dataset``, a trait ``B(A)``
  would make ``Dataset[A]`` a subtype of ``Dataset[B]``. Combine traits with a
  union instead. Any other base class is rejected too, because a ``Field``
  declared on it would silently be left out of the trait's columns.

Reading a trait's columns
-------------------------

A method reads the columns it needs through the trait it asked for:
``HasLeadVariantId.of(ds).lead_variant_id`` is the dataset's column for that
field. ``Trait.of`` only type-checks for a dataset that declares the trait, and
the field resolves to whatever column name the dataset uses, so the method never
spells out a column name. The binding goes from the trait to the dataset, never
``ds.of(Trait)``: with a union of traits on the dataset, a type checker cannot
narrow the result of ``ds.of(Trait)`` to the one trait asked for.

Column names belong to the dataset
----------------------------------

A field's ``name`` is its attribute in camelCase, but the column that stores it
is named by the dataset: the dataset's attribute, in camelCase, is the column
name. ``StudyLocus`` keeps its key as ``id`` by declaring
``id = implements(CanJoinToStudyLocus.study_locus_id)``. The column records the
fields it implements in its metadata, as
``{"traits": ["CanJoinToStudyLocus.study_locus_id"]}``, and ``Trait.of`` follows
that link.

A column only serves a trait field when the dataset links it explicitly. Two
traits can share a column, as in
``variant_id = implements(HasVariantId.variant_id, CanJoinToVariantIndex.variant_id)``,
but a column that merely has a matching name does not count. Two traits that
happen to use the same name for different quantities, such as a marginal and a
conditional ``beta``, therefore never end up reading the same column by
accident.

Where the logic goes
--------------------

A trait carries no logic. Code that derives a value from a column, such as the
position encoded in a variant identifier, is a plain function that takes and
returns a ``Column``. The same function then serves every column that holds that
kind of value: ``variantId``, ``leadVariantId``, and nested ones such as
``ldSet[].tagVariantId`` that no trait can describe.

Example:
    A trait with two columns. The attribute names become camelCase column names,
    and both columns are mandatory:

    >>> from pyspark.sql import types as t
    >>> class HasPValue(Trait):
    ...     p_value_mantissa = Field(t.DoubleType())
    ...     p_value_exponent = Field(t.IntegerType())
    >>> [field.name for field in HasPValue.fields]
    ['pValueMantissa', 'pValueExponent']
    >>> HasPValue.p_value_mantissa.key
    'HasPValue.p_value_mantissa'
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any, ClassVar, Self, overload

from pyspark.sql import types as t

if TYPE_CHECKING:
    from pyspark.sql import Column

    from gentropy.model.core.dataset import Dataset

SNAKE_CASE = re.compile(r"[a-z0-9]+(_[a-z0-9]+)*")

MANDATORY = "mandatory"
"""Metadata key that marks a column as mandatory, i.e. owned by a trait."""

TRAITS = "traits"
"""Metadata key listing the trait fields a column implements, as ``Trait.field``."""


def is_mandatory(field: t.StructField) -> bool:
    """Whether a struct field carries the mandatory marker.

    Args:
        field (t.StructField): Column to inspect.

    Returns:
        bool: True if the column came from a trait.
    """
    return bool(field.metadata.get(MANDATORY, False))


class TraitError(TypeError):
    """A trait or one of its fields is declared incorrectly.

    It is raised when a class is defined and never while data is processed. A
    ``TraitError`` means the code that declares a trait is wrong; the DataFrame it
    would be applied to plays no part.
    """


def to_camel_case(name: str) -> str:
    """Convert a snake_case attribute name to a camelCase column name.

    The first word is kept as it is and every later word starts with a capital
    letter. Digits stay attached to their word. Acronyms come out in lower case,
    since the rule has no way to know about them.

    Args:
        name (str): snake_case name.

    Returns:
        str: camelCase name.

    Examples:
        >>> to_camel_case("study_id")
        'studyId'
        >>> to_camel_case("log10_bayes_factor")
        'log10BayesFactor'
        >>> to_camel_case("qc_flags")
        'qcFlags'
        >>> to_camel_case("beta")
        'beta'
    """
    head, *rest = name.split("_")
    return head + "".join(part[:1].upper() + part[1:] for part in rest)


class Field:
    """One mandatory column of a trait, named after its attribute in camelCase.

    A field holds a Spark type and nothing else. Its name comes from the attribute
    it is assigned to, and it is always mandatory, so a field takes no name or
    nullability argument. A dataset stores it in a column declared with
    ``implements``, under a name of the dataset's choosing. A ``Field`` only
    means something inside a ``Trait`` subclass, which also checks that the
    attribute name is snake_case.

    Examples:
        >>> from pyspark.sql import types as t
        >>> class HasStudyId(Trait):
        ...     study_id = Field(t.StringType())
        >>> HasStudyId.study_id.attribute, HasStudyId.study_id.name
        ('study_id', 'studyId')
        >>> HasStudyId.study_id.key
        'HasStudyId.study_id'

        The type must be a Spark type instance:

        >>> Field(str)
        Traceback (most recent call last):
        ...
        gentropy.model.core.trait.TraitError: Field type must be a Spark DataType, got <class 'str'>
    """

    def __init__(self, dtype: t.DataType) -> None:
        """Declare a trait column of type ``dtype``.

        Args:
            dtype (t.DataType): Spark type of the column.

        Raises:
            TraitError: If ``dtype`` is not a Spark DataType.
        """
        if not isinstance(dtype, t.DataType):
            raise TraitError(f"Field type must be a Spark DataType, got {dtype!r}")
        self.dtype = dtype
        self.attribute = ""
        self.owner: type | None = None

    def __set_name__(self, owner: type, name: str) -> None:
        """Record the attribute name and the declaring class; ``Trait`` validates them.

        Python calls this when the class body that holds the field is executed.

        Args:
            owner (type): Class the field is declared on.
            name (str): Attribute name.
        """
        # A field assigned again in another class body (a mistake ``Dataset``
        # reports) must keep pointing at the trait that declared it.
        if self.owner is None:
            self.attribute = name
            self.owner = owner

    @property
    def name(self) -> str:
        """Default column name: the attribute in camelCase.

        Returns:
            str: Column name.
        """
        return to_camel_case(self.attribute)

    @property
    def key(self) -> str:
        """The trait field this column implements, as ``Trait.attribute``.

        Returns:
            str: Trait name and attribute name.
        """
        owner = self.owner.__name__ if self.owner else ""
        return f"{owner}.{self.attribute}"

    @overload
    def __get__(self, obj: None, owner: type) -> Field: ...
    @overload
    def __get__(self, obj: Trait, owner: type) -> Column: ...
    def __get__(self, obj: Trait | None, owner: type) -> Field | Column:
        """The field itself on the trait class, the dataset's column on a bound trait.

        Args:
            obj (Trait | None): Bound trait, or None for access on the class.
            owner (type): The trait class.

        Returns:
            Field | Column: ``self`` on the class; the column on a bound trait.
        """
        if obj is None:
            return self
        dataset = obj._dataset
        return dataset.df[self.column_in(dataset.schema)]

    def column_in(self, schema: t.StructType) -> str:
        """Name of the column that implements this field in ``schema``.

        Args:
            schema (t.StructType): A dataset schema.

        Returns:
            str: The name of the column linked to this field.

        Raises:
            TraitError: If no column in ``schema`` is linked to this field.
        """
        for column in schema.fields:
            if self.key in column.metadata.get(TRAITS, []):
                return column.name
        raise TraitError(f"no column implements {self.key}")


class Trait:
    """Base class for traits. Subclass it and declare ``Field`` attributes.

    A subclass is a trait. It lists its columns as ``Field`` attributes and nothing
    else. The checks in ``__init_subclass__`` run once, when the subclass is
    defined, and the module docstring explains why each rule exists. After the
    checks, ``fields`` holds the trait's columns in declaration order.

    Examples:
        A well-formed trait and its columns:

        >>> from pyspark.sql import types as t
        >>> class HasRegion(Trait):
        ...     chromosome = Field(t.StringType())
        ...     locus_start = Field(t.IntegerType())
        ...     locus_end = Field(t.IntegerType())
        >>> [field.name for field in HasRegion.fields]
        ['chromosome', 'locusStart', 'locusEnd']

        A trait must declare at least one column:

        >>> class HasNothing(Trait):
        ...     pass
        Traceback (most recent call last):
        ...
        gentropy.model.core.trait.TraitError: HasNothing: a trait must declare at least one Field

        Field attributes must be snake_case:

        >>> class HasStudy(Trait):
        ...     studyId = Field(t.StringType())
        Traceback (most recent call last):
        ...
        gentropy.model.core.trait.TraitError: HasStudy.studyId: field attributes must be snake_case so they map to a camelCase column name

        Traits combine through unions, never through inheritance:

        >>> class HasRegionAndLead(HasRegion):
        ...     lead_variant_id = Field(t.StringType())
        Traceback (most recent call last):
        ...
        gentropy.model.core.trait.TraitError: HasRegionAndLead: a trait must subclass Trait directly and nothing else; combine traits with a union, e.g. `Dataset[A | B]`
    """

    fields: ClassVar[tuple[Field, ...]] = ()
    """The trait's fields in declaration order. Set once, when the class is defined."""

    _dataset: Dataset[Self]
    """The dataset a view reads from. Set only by ``Trait.of``."""

    def __init__(self) -> None:
        """Refuse direct construction: views of a trait come from ``Trait.of``.

        Raises:
            TraitError: Always.
        """
        raise TraitError(
            f"{type(self).__name__} is a trait declaration; "
            f"use {type(self).__name__}.of(dataset) to read a dataset through it"
        )

    @classmethod
    def of(cls, dataset: Dataset[Self]) -> Self:
        """This trait bound to ``dataset``, so its fields read the dataset's columns.

        A field read from the bound trait is the dataset's ``Column`` for it,
        under whatever name the dataset stores it.

        Examples:
            >>> from pyspark.sql import types as t
            >>> from gentropy.model.core.dataset import Dataset
            >>> from gentropy.model.core.schema import implements
            >>> class HasStudyId(Trait):
            ...     study_id = Field(t.StringType())
            >>> class Studies(Dataset[HasStudyId]):
            ...     id = implements(HasStudyId.study_id)
            >>> studies = Studies(spark.createDataFrame([("GCST1",)], "id string"))
            >>> studies.df.select(HasStudyId.of(studies).study_id).first()
            Row(id='GCST1')

            A dataset that does not declare the trait cannot be bound to it:

            >>> class HasBeta(Trait):
            ...     beta = Field(t.DoubleType())
            >>> HasBeta.of(studies)
            Traceback (most recent call last):
            ...
            gentropy.model.core.trait.TraitError: Studies does not implement HasBeta

        Args:
            dataset (Dataset[Self]): A dataset that implements this trait.

        Returns:
            Self: The bound trait.

        Raises:
            TraitError: If the dataset does not declare this trait.
        """
        if cls not in type(dataset).traits:
            raise TraitError(
                f"{type(dataset).__name__} does not implement {cls.__name__}"
            )
        # The view is an instance of the trait class, so that the Field descriptors
        # type its attributes as Columns. It is built without __init__, which only
        # exists to stop direct construction.
        view = cls.__new__(cls)
        view._dataset = dataset
        return view

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """Check the trait declaration when the class is defined.

        Args:
            **kwargs (Any): Passed on to ``object.__init_subclass__``.

        Raises:
            TraitError: If the trait has a base other than ``Trait``, declares no
                fields, or names a field with an attribute that is not snake_case.
        """
        super().__init_subclass__(**kwargs)
        # Another trait as base would reverse the contravariant subtyping, and a
        # Field on any other base would be left out of ``fields``.
        if cls.__bases__ != (Trait,):
            raise TraitError(
                f"{cls.__name__}: a trait must subclass Trait directly and nothing "
                "else; combine traits with a union, e.g. `Dataset[A | B]`"
            )
        fields = tuple(v for v in vars(cls).values() if isinstance(v, Field))
        if not fields:
            raise TraitError(f"{cls.__name__}: a trait must declare at least one Field")
        # Validated here, not in Field.__set_name__: Python 3.11 wraps errors raised there.
        for field in fields:
            if not SNAKE_CASE.fullmatch(field.attribute):
                raise TraitError(
                    f"{cls.__name__}.{field.attribute}: field attributes must be "
                    "snake_case so they map to a camelCase column name"
                )
        cls.fields = fields
