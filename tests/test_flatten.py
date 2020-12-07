from pyspark.sql.types import StructType, ArrayType, StructField, StringType

from tests.fixtures import local_spark_session, sample_dataframe
from flapjack.flatten import Extractor


def test_expand_struct(sample_dataframe):
    expected_schema = StructType(
        [
            StructField(
                "vehicle",
                ArrayType(
                    StructType(
                        [
                            StructField("vin", StringType(), True),
                            StructField(
                                "wheels",
                                ArrayType(
                                    StructType([StructField("id", StringType(), True)])
                                ),
                                True,
                            ),
                        ]
                    )
                ),
                True,
            ),
            StructField("policy_policyNumber", StringType(), True),
            StructField(
                "policy_statusCd",
                StructType(
                    [
                        StructField("srcCd", StringType(), True),
                        StructField("value", StringType(), True),
                    ]
                ),
                True,
            ),
        ]
    )

    result = Extractor.expand_struct("policy", sample_dataframe)
    assert result.schema == expected_schema
    assert result.count() == sample_dataframe.count()


def test_explode_array(sample_dataframe):
    expected_schema = StructType(
        [
            StructField(
                "policy",
                StructType(
                    [
                        StructField("policyNumber", StringType(), True),
                        StructField(
                            "statusCd",
                            StructType(
                                [
                                    StructField("srcCd", StringType(), True),
                                    StructField("value", StringType(), True),
                                ]
                            ),
                            True,
                        ),
                    ]
                ),
                True,
            ),
            StructField("vehicle_vin", StringType(), True),
            StructField(
                "vehicle_wheels",
                ArrayType(StructType([StructField("id", StringType(), True)])),
                True,
            ),
        ]
    )

    result = Extractor.explode_array("vehicle", sample_dataframe)
    assert result.schema == expected_schema
    assert result.count() >= sample_dataframe.count()


def test_expand_array(sample_dataframe):
    expected_schema = StructType(
        [
            StructField("policy_policyNumber", StringType(), True),
            StructField("policy_statusCd_srcCd", StringType(), True),
            StructField("policy_statusCd_value", StringType(), True),
            StructField("vehicle_1_vin", StringType(), True),
            StructField(
                "vehicle_1_wheels",
                ArrayType(StructType([StructField("id", StringType(), True)])),
                True,
            ),
            StructField("vehicle_2_vin", StringType(), True),
            StructField(
                "vehicle_2_wheels",
                ArrayType(StructType([StructField("id", StringType(), True)])),
                True,
            ),
        ]
    )

    result = Extractor.expand_array("vehicle", sample_dataframe)
    assert result.schema == expected_schema
    assert result.count() == sample_dataframe.count()


def test_expand_array_capped(sample_dataframe):
    expected_schema = StructType(
        [
            StructField("policy_policyNumber", StringType(), True),
            StructField("policy_statusCd_srcCd", StringType(), True),
            StructField("policy_statusCd_value", StringType(), True),
            StructField("vehicle_1_vin", StringType(), True),
            StructField(
                "vehicle_1_wheels",
                ArrayType(StructType([StructField("id", StringType(), True)])),
                True,
            ),
        ]
    )

    result = Extractor.expand_array("vehicle", sample_dataframe, max_elements=1)
    assert result.schema == expected_schema
    assert result.count() == sample_dataframe.count()


def test_recursive_expand_struct(sample_dataframe):
    expected_schema = StructType(
        [
            StructField("policy_policyNumber", StringType(), True),
            StructField("policy_statusCd_srcCd", StringType(), True),
            StructField("policy_statusCd_value", StringType(), True),
            StructField(
                "vehicle",
                ArrayType(
                    StructType(
                        [
                            StructField("vin", StringType(), True),
                            StructField(
                                "wheels",
                                ArrayType(
                                    StructType([StructField("id", StringType(), True)])
                                ),
                                True,
                            ),
                        ]
                    )
                ),
                True,
            ),
        ]
    )

    result = Extractor.recursive_expand_struct(sample_dataframe)
    assert result.schema == expected_schema
    assert result.count() == sample_dataframe.count()
