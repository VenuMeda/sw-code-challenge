# coding=utf-8

import logging
import pytest

from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql.session import SparkSession


# @pytest.fixture(scope="session")
# def spark_context(request):
#     """ fixture for creating a spark context.
#     Re-use spark_context fixtures across all tests in a session
#     Args:
#         request: pytest.FixtureRequest object
#     """
#     conf = (SparkConf().setMaster("local[2]").setAppName("pytest-pyspark-local-testing"))
#     sc = SparkContext(conf=conf)
#     request.addfinalizer(lambda: sc.stop())
#
#     return sc


@pytest.fixture(scope="session")
def spark_session(request):
    """ fixture for creating a spark session.
    Re-use spark_session fixtures across all tests in a session
    Args:
        request: pytest.FixtureRequest object
    """
    spark_session = SparkSession.builder \
        .master("local[*]") \
        .appName("pytest-pyspark-local-testing") \
        .getOrCreate()
    request.addfinalizer(lambda: spark_session.stop())

    return spark_session
