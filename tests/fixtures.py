import pytest
from dataclasses import dataclass
from typing import List
from pyspark import SparkContext
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def local_spark_session(request):
    def shush_spark(sc: SparkContext):
        logger = sc._jvm.org.apache.log4j
        logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
        logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

    spark = (
        SparkSession.builder.master("local[1]")
        .config("spark.driver.cores", "1")
        .config("spark.executor.cores", "1")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.port.maxRetries", "100")
        .config("spark.driver.bindAddress", "localhost")
        .config("spark.driver.host", "localhost")
        .getOrCreate()
    )
    shush_spark(spark.sparkContext)
    yield spark
    spark.stop()


@pytest.fixture(scope="session")
def sample_dataframe(local_spark_session):
    @dataclass
    class CodeField:
        srcCd: str
        value: str

    @dataclass
    class Policy:
        policyNumber: str
        statusCd: CodeField

    @dataclass
    class Wheel:
        id: str

    @dataclass
    class Vehicle:
        vin: str
        wheels: List[Wheel]

    @dataclass
    class Root:
        policy: Policy
        vehicle: List[Vehicle]

    df = local_spark_session.createDataFrame(
        [
            Root(
                Policy("P1", CodeField("1", "01")),
                [
                    Vehicle("V1", [Wheel("FL"), Wheel("FR"), Wheel("BL"), Wheel("BR")]),
                    Vehicle("V2", [Wheel("FL"), Wheel("FR"), Wheel("BL"), Wheel("BR")]),
                ],
            ),
            Root(
                Policy("P2", CodeField("2", "02")),
                [Vehicle("V1", [Wheel("FC"), Wheel("BL"), Wheel("BR")])],
            ),
        ]
    )
    return df
