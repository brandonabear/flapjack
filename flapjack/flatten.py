from collections import OrderedDict
from typing import List, Tuple
from pyspark.sql import DataFrame, Column
from pyspark.sql.functions import col, explode_outer, max, size
from pyspark.sql.types import DataType, StructField, StructType


class Extractor:
    """Utility class for exposing standardized flattening functions.

    Includes the following:
        * recursive_expand_struct
        * expand_struct
        * expand_array
        * explode_array
    """

    @classmethod
    def recursive_expand_struct(cls, df: DataFrame) -> DataFrame:
        def _is_struct(sf: StructField) -> bool:
            """Returns true if the field is a struct."""
            return isinstance(sf.dataType, StructType)

        def _has_struct(st: StructType) -> bool:
            """Returns true if any field contains a struct."""
            return any([_is_struct(f) for f in st.fields])

        def _flatten_one_level(
            st: StructType,
            parent: Column = None,
            prefix: str = None,
            cols: List[Tuple[DataType, Column]] = [],
        ) -> List[Tuple[DataType, Column]]:
            columns = cols if cols else []
            for f in st.fields:

                temp_col = (
                    parent.getItem(f.name)
                    if isinstance(parent, Column)
                    else col(f.name)
                )
                flat_name = f"{prefix}_{f.name}" if prefix else f.name

                if isinstance(f.dataType, StructType):
                    columns = cols + _flatten_one_level(
                        f.dataType, temp_col, flat_name, columns
                    )
                else:
                    columns.append((f.dataType, temp_col.alias(flat_name)))

            return columns

        flat_df = df
        while _has_struct(flat_df.schema):
            new_fields = _flatten_one_level(flat_df.schema)
            new_columns = [f[1] for f in new_fields]
            flat_df = flat_df.select(*new_columns)

        unique_columns = list(OrderedDict.fromkeys(flat_df.columns))
        return flat_df.select(unique_columns)

    @staticmethod
    def expand_struct(struct_name: str, df: DataFrame) -> DataFrame:
        inner_columns = df.select(f"{struct_name}.*").columns
        return df.select(
            [col("*")]
            + [
                col(f"{struct_name}.{c}").alias(f"{struct_name}_{c}")
                for c in inner_columns
            ]
        ).drop(struct_name)

    @classmethod
    def explode_array(cls, array_name: str, df: DataFrame) -> DataFrame:
        exploded_array = df.withColumn(array_name, explode_outer(col(array_name)))
        expanded_struct = cls.expand_struct(array_name, exploded_array)
        return expanded_struct.drop(array_name)

    @classmethod
    def expand_array(
        cls, array_name: str, df: DataFrame, max_elements: int = None
    ) -> DataFrame:
        if not max_elements:
            array_cap = int(df.agg(max(size(col(array_name)))).collect()[0][0])
        else:
            array_cap = max_elements

        indexed_array = df.select(
            [col("*")]
            + [
                col(array_name).getItem(i).alias(f"{array_name}_{i+1}")
                for i in range(0, array_cap)
            ]
        ).drop(array_name)
        return cls.recursive_expand_struct(indexed_array)
