# coding=utf-8

import logging
import pytest

from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql.session import SparkSession


def non_verbose_py4j():
    """ turn off spark logging for the test context """
    logger = logging.getLogger('py4j')
    logger.setLevel(logging.WARN)


@pytest.fixture(scope="session")
def spark_context(request):
    """ fixture for creating a spark context.
    Re-use spark_context fixtures across all tests in a session
    Args:
        request: pytest.FixtureRequest object
    """
    conf = (SparkConf().setMaster("local[2]").setAppName("pytest-pyspark-local-testing"))
    sc = SparkContext(conf=conf)
    request.addfinalizer(lambda: sc.stop())

    # non_verbose_py4j()
    return sc


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

    # non_verbose_py4j()
    return spark_session
