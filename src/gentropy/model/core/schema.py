"""Dataset columns: how a dataset declares its layout, one class attribute each.

A dataset lists its columns in its class body. Each attribute becomes one column,
named after the attribute in camelCase, and the order of the attributes is the
order of the columns. There are three kinds:

* ``optional(dtype)`` declares a column no trait requires. A DataFrame may leave
  it out.
* ``required(dtype)`` declares a column the dataset itself requires, without a
  trait. A DataFrame must have it, but no method can ask for it through a trait,
  so it suits datasets that are only ever used whole, such as the LD index. Its
  metadata is ``{"mandatory": true}``.
* ``implements(field, ...)`` declares a mandatory column that stores one or more
  trait fields. Its type is the fields' type, and its metadata records which
  fields it stores, as ``{"mandatory": true, "traits": ["Trait.field", ...]}``.
  ``Trait.of(ds).field`` and the dataset's checks follow that link.

Every column is nullable, which matches what Spark produces when it reads
parquet. Whether a column is mandatory lives only in the metadata.

A column only serves a trait field the dataset names in ``implements``. Two traits
that share a column are listed together in one ``implements`` call, and their
fields must hold the same type.

Reading an attribute from a dataset instance gives its DataFrame column, so
dataset-specific code refers to columns by attribute and a typo fails type
checking. Code written against traits uses ``Trait.of(ds)`` instead.

Example:
    >>> from pyspark.sql import types as t
    >>> from gentropy.model.core.dataset import Dataset
    >>> from gentropy.model.core.trait import Field, Trait
    >>> class HasVariantId(Trait):
    ...     variant_id = Field(t.StringType())
    >>> class CanJoinToVariantIndex(Trait):
    ...     variant_id = Field(t.StringType())
    >>> class Variants(Dataset[HasVariantId | CanJoinToVariantIndex]):
    ...     variant_id = implements(
    ...         HasVariantId.variant_id, CanJoinToVariantIndex.variant_id
    ...     )
    ...     most_severe_consequence = optional(t.StringType())
    >>> Variants.print_schema()
    Variants
     |-- variantId: string (mandatory: HasVariantId.variant_id, CanJoinToVariantIndex.variant_id)
     |-- mostSevereConsequence: string (optional)
    >>> variants = Variants(spark.createDataFrame([("1_100_A_T",)], "variantId string"))
    >>> variants.df.select(variants.variant_id).first()
    Row(variantId='1_100_A_T')
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, overload

from pyspark.sql import types as t

from gentropy.model.core.trait import MANDATORY, TRAITS, Field, Trait, to_camel_case

if TYPE_CHECKING:
    from pyspark.sql import Column

    from gentropy.model.core.dataset import Dataset


class SchemaError(TypeError):
    """A dataset column is declared incorrectly.

    Like ``TraitError``, it is raised while classes are being defined. It means the
    column declaration is wrong; no data is involved.
    """


class DatasetColumn:
    """One column of a dataset, declared as a class attribute.

    The attribute name, converted to camelCase, is the column name. A column that
    implements trait fields is mandatory and records them in its metadata; any
    other column is optional unless declared required. Build one with
    ``optional``, ``required`` or ``implements``.
    """

    def __init__(
        self,
        dtype: t.DataType,
        links: tuple[Field, ...] = (),
        mandatory: bool = False,
    ) -> None:
        """Declare a column of type ``dtype`` implementing ``links``.

        Args:
            dtype (t.DataType): Spark type of the column.
            links (tuple[Field, ...]): Trait fields the column implements.
            mandatory (bool): Whether the column is mandatory without a trait link.
        """
        self.dtype = dtype
        self.links = links
        self.mandatory = mandatory or bool(links)
        self.attribute = ""

    def __set_name__(self, owner: type, name: str) -> None:
        """Record the attribute name, which becomes the column name.

        Args:
            owner (type): Class the column is declared on.
            name (str): Attribute name.
        """
        self.attribute = name

    @overload
    def __get__(self, obj: None, owner: type) -> DatasetColumn: ...
    @overload
    def __get__(self, obj: Dataset[Any], owner: type) -> Column: ...
    def __get__(self, obj: Dataset[Any] | None, owner: type) -> DatasetColumn | Column:
        """The declaration on the class, the DataFrame column on a dataset.

        Args:
            obj (Dataset[Any] | None): Dataset instance, or None on the class.
            owner (type): The dataset class.

        Returns:
            DatasetColumn | Column: ``self`` on the class; the column on a dataset.
        """
        return self if obj is None else obj.df[self.name]

    @property
    def name(self) -> str:
        """Column name: the attribute name in camelCase.

        Returns:
            str: Column name.
        """
        return to_camel_case(self.attribute)

    @property
    def struct_field(self) -> t.StructField:
        """The column as a nullable struct field, marked mandatory when it is.

        Returns:
            t.StructField: The column's struct field.
        """
        metadata: dict[str, Any] = {MANDATORY: True} if self.mandatory else {}
        if self.links:
            metadata[TRAITS] = [field.key for field in self.links]
        return t.StructField(self.name, self.dtype, nullable=True, metadata=metadata)


def optional(dtype: t.DataType) -> DatasetColumn:
    """Declare an optional column, which no trait requires.

    Args:
        dtype (t.DataType): Spark type of the column.

    Returns:
        DatasetColumn: The column declaration.

    Raises:
        SchemaError: If ``dtype`` is not a Spark DataType.
    """
    if not isinstance(dtype, t.DataType):
        raise SchemaError(f"Column type must be a Spark DataType, got {dtype!r}")
    return DatasetColumn(dtype)


def required(dtype: t.DataType) -> DatasetColumn:
    """Declare a mandatory column that no trait requires.

    Args:
        dtype (t.DataType): Spark type of the column.

    Returns:
        DatasetColumn: The column declaration.

    Raises:
        SchemaError: If ``dtype`` is not a Spark DataType.

    Examples:
        >>> from pyspark.sql import types as t
        >>> required(t.StringType()).mandatory
        True
    """
    if not isinstance(dtype, t.DataType):
        raise SchemaError(f"Column type must be a Spark DataType, got {dtype!r}")
    return DatasetColumn(dtype, mandatory=True)


def implements(*fields: Field) -> DatasetColumn:
    """Declare a mandatory column that implements one or more trait fields.

    Args:
        *fields (Field): Trait fields stored in this column.

    Returns:
        DatasetColumn: The column declaration.

    Raises:
        SchemaError: If no field is given, a field is not declared on a trait, or
            the fields hold different types.

    Examples:
        >>> from pyspark.sql import types as t
        >>> from gentropy.model.core.trait import Field, Trait
        >>> class HasVariantId(Trait):
        ...     variant_id = Field(t.StringType())
        >>> class HasPosition(Trait):
        ...     position = Field(t.IntegerType())
        >>> implements(HasVariantId.variant_id, HasPosition.position)
        Traceback (most recent call last):
        ...
        gentropy.model.core.schema.SchemaError: HasVariantId.variant_id is string but HasPosition.position is int; fields sharing a column must hold the same type
    """
    if not fields:
        raise SchemaError("implements() needs at least one trait field")
    for field in fields:
        if field.owner is None or not issubclass(field.owner, Trait):
            raise SchemaError(
                f"{field.dtype.simpleString()} field is not declared on a trait"
            )
    first = fields[0]
    for field in fields[1:]:
        if field.dtype != first.dtype:
            raise SchemaError(
                f"{first.key} is {first.dtype.simpleString()} but "
                f"{field.key} is {field.dtype.simpleString()}; "
                "fields sharing a column must hold the same type"
            )
    return DatasetColumn(first.dtype, fields)
