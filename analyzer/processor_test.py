"""
This file contains Test cases of analyzer for processing NASA Web Server Log files
"""
from unittest import mock

import pandas as pd
import pytest

from . import processor

# this allows using the fixture in all test cases in this module
pytestmark = pytest.mark.usefixtures("spark_session")


# def test_parser(spark_session):
def test_parser_hosts():
    args = processor.parse_args(['hosts', '--top', '3'])
    assert args.command == 'hosts'
    assert args.top == 3
    assert args.csv == False


def test_parser_urls():
    args = processor.parse_args(['urls', '--top', '3'])
    assert args.command == 'urls'
    assert args.top == 3
    assert args.csv == False


def test_drop_rows_with_nulls_any_column(spark_session):
    '''
    This test if rows with nulls in any column are discorded or not

    :param spark_session:
    :return:
    '''
    print("setting test input")

    test_input = [
        ('d104.aa.net', 'GET'),
        (None, 'GET'),
    ]

    expected_out = [
        ('d104.aa.net', 'GET')
    ]

    in_df = spark_session.createDataFrame(data=test_input, schema=['host', 'method'])
    expect_out_df = spark_session.createDataFrame(data=expected_out, schema=['host', 'method'])

    actual_output_df = processor.drop_rows_with_nulls_any_column(in_df)
    print("Expected output dataframe")
    expect_out_df.show(truncate=False)
    print("Actual output dataframe")
    actual_output_df.show(truncate=False)

    actual_output_df = get_sorted_data_frame(actual_output_df.toPandas(), actual_output_df.columns)
    expected_output_df = get_sorted_data_frame(expect_out_df.toPandas(), expect_out_df.columns)
    pd.testing.assert_frame_equal(expected_output_df, actual_output_df, check_like=True)


def test_replace_null_values(spark_session):
    '''
    This tests if null values are replaced with Zeors in dataframe
    :param spark_session:
    :return:
    '''

    print("setting test input")
    test_input = [
        ('d104.aa.net', 'GET', 2048),
        (None, 'GET', None),
        ('120.120.120.120', 'GET', None),
    ]

    expected_out = [
        ('d104.aa.net', 'GET', 2048),
        (None, 'GET', 0),
        ('120.120.120.120', 'GET', 0),

    ]

    expected_output_df = spark_session.createDataFrame(data=test_input, schema=['host', 'method', 'content_size'])
    expect_out_df = spark_session.createDataFrame(data=expected_out, schema=['host', 'method', 'content_size'])

    # Test replace nulls with zeros for content_size.
    actual_output_df = processor.replace_null_with_value(expected_output_df, 'content_size', 0)
    print("Expected output dataframe")
    expected_output_df.show(truncate=False)
    print("Actual output dataframe")
    actual_output_df.show(truncate=False)

    actual_output_df = get_sorted_data_frame(actual_output_df.toPandas(), actual_output_df.columns)
    expected_output_df = get_sorted_data_frame(expect_out_df.toPandas(), expect_out_df.columns)

    pd.testing.assert_frame_equal(expected_output_df, actual_output_df, check_like=True)


def test_extract(spark_session):
    """ test that a set of log entries are parsed correctly
    Tested if host, timestamp, method, uri, protocol, status code, and content sizes are
    extracted as desired.
        Args:
            spark_session: test fixture SparkContext
        """

    from pyspark.sql.types import StringType
    print("setting test input")
    test_input = [
        'd104.aa.net - - [01/Jul/1995:00:00:13 -0400] "GET /shuttle/countdown/ HTTP/1.0" 200 2048',
        'invalidhost - - [01/Jul/1995:00:00:13 -0400] "GET /shuttle/countdown/ HTTP/1.0" 404 -',
        '... - - [01/Jul/1995:00:00:13 -0400] "GET /shuttle/countdown/ HTTP/1.0" 4   -',
        '.  .  .   - - [01/Jul/1995:00:00:13 -0400] "GET /shuttle/countdown/ HTTP/1.0" 404 -',
        '.  .host.domain.com   - - [01/Jul/1995:00:00:13 -0400] "GET /shuttle/countdown/ HTTP/1.0" 404 -',
        '.  .host.domain.com   - - [01/Jul/1995:00:00: -0400] "GET /shuttle/countdown/ HTTP/1.0" 404 -',
        '  .host.domain.com   - - [/Jul/1995:00:00:13 -0400] "GET /shuttle/countdown/ HTTP/1.0" 404 -',
        'it.is.valid.host.domain.com - - [In/Val/idda:te:00:13 -0400] "GET /shuttle/countdown/ HTTP/1.0" 200 2048',
        'it.is.valid.host.domain.com - - [In/Val/idda:te:00:13 -0400] "GET / HTTP/1.0" 200 2048',
        'it.is.valid.host.domain.com - - [In/Val/idda:te:00:13 -0400] "GET  HTTP/1.0" 200 2048',
    ]

    in_df = spark_session.createDataFrame(test_input, StringType())
    in_df.show(truncate=False)

    expected_out = [
        ('d104.aa.net', '01/Jul/1995:00:00:13 -0400', 'GET', '/shuttle/countdown/', 'HTTP/1.0', 200, 2048),
        ('', '01/Jul/1995:00:00:13 -0400', 'GET', '/shuttle/countdown/', 'HTTP/1.0', 404, None),
        ('', '01/Jul/1995:00:00:13 -0400', 'GET', '/shuttle/countdown/', 'HTTP/1.0', None, None),
        ('', '01/Jul/1995:00:00:13 -0400', 'GET', '/shuttle/countdown/', 'HTTP/1.0', 404, None),
        ('', '01/Jul/1995:00:00:13 -0400', 'GET', '/shuttle/countdown/', 'HTTP/1.0', 404, None),
        ('', '', 'GET', '/shuttle/countdown/', 'HTTP/1.0', 404, None),
        ('', '', 'GET', '/shuttle/countdown/', 'HTTP/1.0', 404, None),
        ('it.is.valid.host.domain.com', '', 'GET', '/shuttle/countdown/', 'HTTP/1.0', 200, 2048),
        ('it.is.valid.host.domain.com', '', 'GET', '/', 'HTTP/1.0', 200, 2048),
        ('it.is.valid.host.domain.com', '', '', '', '', 200, 2048),
    ]

    expect_out_df = spark_session.createDataFrame(data=expected_out,
                                                  schema=['host', 'time', 'method', 'endpoint', 'protocol', 'status',
                                                          'content_size'])

    print("Expected output dataframe")
    expect_out_df.show(truncate=False)

    actual_output_df = processor.extract(in_df)
    print("Actual output dataframe")
    actual_output_df.show(truncate=False)

    actual_output_df = get_sorted_data_frame(actual_output_df.toPandas(), actual_output_df.columns)
    expected_output_df = get_sorted_data_frame(expect_out_df.toPandas(), expect_out_df.columns)

    pd.testing.assert_frame_equal(expected_output_df, actual_output_df, check_like=True, check_dtype=False)


def test_transform(spark_session):
    """ test dataframe is transformed to desired state. Here, desired state is no null values
    for the host, method, endpoint, protocol, status, content_size >=0, and date in Date
    format without time.
        Args:
            spark_session: test fixture SparkContext
        """

    test_input = [
        ('d104.aa.net', '01/Jul/1995:00:00:13 -0400', 'GET', '/shuttle/countdown/', 'HTTP/1.0', 200, 2048),
        ('d104.aa.net', '01/Jul/1995:00:00:13 -0400', 'GET', '/shuttle/countdown/', 'HTTP/1.0', 300, None),
        ('', '01/Jul/1995:00:00:13 -0400', 'GET', '/shuttle/countdown/', 'HTTP/1.0', 404, None),
        ('', '01/Jul/1995:00:00:13 -0400', 'GET', '/shuttle/countdown/', 'HTTP/1.0', None, None),
        ('', '01/Jul/1995:00:00:13 -0400', 'GET', '/shuttle/countdown/', 'HTTP/1.0', 404, None),
        ('', '01/Jul/1995:00:00:13 -0400', 'GET', '/shuttle/countdown/', 'HTTP/1.0', 404, None),
        ('', '', 'GET', '/shuttle/countdown/', 'HTTP/1.0', 404, None),
        ('', '', 'GET', '/shuttle/countdown/', 'HTTP/1.0', 404, None),
        ('it.is.valid.host.domain.com', '', 'GET', '/shuttle/countdown/', 'HTTP/1.0', 200, 2048),
        ('it.is.valid.host.domain.com', '', 'GET', '/', 'HTTP/1.0', 200, 2048),
        ('it.is.valid.host.domain.com', '', '', '', '', 200, 2048),
    ]
    test_input_df = spark_session.createDataFrame(test_input,
                                                  ['host', 'time', 'method', 'endpoint', 'protocol', 'status',
                                                   'content_size'])

    expected_out = [
        ('d104.aa.net', 'GET', '/shuttle/countdown/', 'HTTP/1.0', 200, 2048, '1995-07-01'),
        ('d104.aa.net', 'GET', '/shuttle/countdown/', 'HTTP/1.0', 300, 0, '1995-07-01'),
    ]

    expect_out_df = spark_session.createDataFrame(data=expected_out,
                                                  schema=['host', 'method', 'endpoint', 'protocol', 'status',
                                                          'content_size', 'date_str'])
    from pyspark.sql.functions import col
    from pyspark.sql.functions import to_date

    expect_out_df = expect_out_df.withColumn('date', to_date(col('date_str'), 'yyyy-MM-dd')).drop('date_str')

    print("Expected output dataframe")
    expect_out_df.show(truncate=False)

    actual_output_df = processor.transform(test_input_df)

    print("Actual output dataframe")
    actual_output_df.show(truncate=False)

    expected_output_df = get_sorted_data_frame(expect_out_df.toPandas(), expect_out_df.columns)
    actual_output_df = get_sorted_data_frame(actual_output_df.toPandas(), actual_output_df.columns)
    pd.testing.assert_frame_equal(expected_output_df, actual_output_df, check_like=True, check_dtype=False)


def test_fetchTopKUrlsPerDay(spark_session):
    '''

    This method test if top k urls are fetched from test input dataframe

    :param spark_session:
    :return:
    '''

    test_input = [
        ('d104.aa.net', 'GET', '/shuttle/countdown/', 'HTTP/1.0', 200, 2048, '1995-07-01'),
        ('d104.aa.net', 'GET', '/shuttle/countdown/', 'HTTP/1.0', 200, 512, '1995-07-01'),
        ('d104.aa.net', 'GET', '/shuttle/countdown/', 'HTTP/1.0', 300, 0, '1995-07-01'),
        ('d104.aa.net', 'GET', '/shuttle/countdown/', 'HTTP/1.0', 200, 2048, '1995-07-01'),
        ('120.170.19.95', 'GET', '/countdown/', 'HTTP/1.0', 200, 0, '1995-07-01'),
        ('120.170.19.95', 'GET', '/countdown/', 'HTTP/1.0', 400, 0, '1995-07-01'),
        ('120.170.19.95', 'GET', '/home/', 'HTTP/1.0', 300, 0, '1995-07-01'),
        ('node3.xx.org', 'GET', '/index/', 'HTTP/1.0', 200, 0, '1995-07-01'),
        ('node3.xx.org', 'GET', '/', 'HTTP/1.0', 200, 0, '1995-07-01'),
    ]

    test_input_df = spark_session.createDataFrame(data=test_input,
                                                  schema=['host', 'method', 'endpoint', 'protocol', 'status',
                                                          'content_size', 'date_str'])
    from pyspark.sql.functions import col
    from pyspark.sql.functions import to_date

    test_input_df = test_input_df.withColumn('date', to_date(col('date_str'), 'yyyy-MM-dd')).drop('date_str')

    # build expected output dataframe
    expected_output = [
        ('1995-07-01', '/shuttle/countdown/', 4),
        ('1995-07-01', '/countdown/', 2),
    ]
    expect_out_df = spark_session.createDataFrame(data=expected_output,
                                                  schema=['date_str', 'endpoint', 'hits'])
    expect_out_df = expect_out_df.withColumn('date', to_date(col('date_str'), 'yyyy-MM-dd')).drop('date_str')

    print("Expected output dataframe")
    expect_out_df.show(truncate=False)

    actual_output_df = processor.urlsTopNPerDay(test_input_df, 2)

    print("Actual output dataframe")
    actual_output_df.show(truncate=False)

    expect_out_df = get_sorted_data_frame(expect_out_df.toPandas(), expect_out_df.columns)
    actual_output_df = get_sorted_data_frame(actual_output_df.toPandas(), actual_output_df.columns)

    pd.testing.assert_frame_equal(expect_out_df, actual_output_df, check_like=True, check_dtype=False)


def test_fetchTopKUrlsPerDayTieCase(spark_session):
    '''
    This method test if top k urls are fetched from test input dataframe when there
    is tie among the urls. i.e when count matches.

    :param spark_session:
    :return:
    '''
    test_input = [
        ('d104.aa.net', 'GET', '/shuttle/countdown/', 'HTTP/1.0', 200, 2048, '1995-07-01'),
        ('d104.aa.net', 'GET', '/shuttle/countdown/', 'HTTP/1.0', 200, 512, '1995-07-01'),
        ('d104.aa.net', 'GET', '/shuttle/countdown/', 'HTTP/1.0', 300, 0, '1995-07-01'),
        ('d104.aa.net', 'GET', '/shuttle/countdown/', 'HTTP/1.0', 200, 2048, '1995-07-01'),
        ('120.170.19.95', 'GET', '/countdown/', 'HTTP/1.0', 200, 0, '1995-07-01'),
        ('120.170.19.95', 'GET', '/countdown/', 'HTTP/1.0', 400, 0, '1995-07-01'),
        ('120.170.19.95', 'GET', '/home/', 'HTTP/1.0', 300, 0, '1995-07-01'),
        ('node3.xx.org', 'GET', '/index/', 'HTTP/1.0', 200, 0, '1995-07-01'),
        ('node3.xx.org', 'GET', '/index/', 'HTTP/1.0', 200, 0, '1995-07-01'),
        ('node3.xx.org', 'GET', '/', 'HTTP/1.0', 200, 0, '1995-07-01'),
    ]

    test_input_df = spark_session.createDataFrame(data=test_input,
                                                  schema=['host', 'method', 'endpoint', 'protocol', 'status',
                                                          'content_size', 'date_str'])
    from pyspark.sql.functions import col
    from pyspark.sql.functions import to_date

    test_input_df = test_input_df.withColumn('date', to_date(col('date_str'), 'yyyy-MM-dd')).drop('date_str')

    # build expected output dataframe
    expected_output = [
        ('1995-07-01', '/shuttle/countdown/', 4),
        ('1995-07-01', '/countdown/', 2),
        ('1995-07-01', '/index/', 2),
    ]
    expect_out_df = spark_session.createDataFrame(data=expected_output,
                                                  schema=['date_str', 'endpoint', 'hits'])
    expect_out_df = expect_out_df.withColumn('date', to_date(col('date_str'), 'yyyy-MM-dd')).drop('date_str')

    print("Expected output dataframe")
    expect_out_df.show(truncate=False)

    actual_output_df = processor.urlsTopNPerDay(test_input_df, 2)

    print("Actual output dataframe")
    actual_output_df.show(truncate=False)

    expect_out_df = get_sorted_data_frame(expect_out_df.toPandas(), expect_out_df.columns)
    actual_output_df = get_sorted_data_frame(actual_output_df.toPandas(), actual_output_df.columns)

    pd.testing.assert_frame_equal(expect_out_df, actual_output_df, check_like=True, check_dtype=False)


def test_fetchTopKUrlsPerDayWhenLessRecordsThanK(spark_session):
    '''
    This method test if top k urls are fetched from test input dataframe when there
    is tie among the urls. i.e when count matches.

    :param spark_session:
    :return:
    '''
    test_input = [
        ('d104.aa.net', 'GET', '/shuttle/countdown/', 'HTTP/1.0', 200, 2048, '1995-07-01'),
        ('d104.aa.net', 'GET', '/shuttle/countdown/', 'HTTP/1.0', 200, 512, '1995-07-01'),
        ('d104.aa.net', 'GET', '/shuttle/countdown/', 'HTTP/1.0', 300, 0, '1995-07-01'),
        ('d104.aa.net', 'GET', '/shuttle/countdown/', 'HTTP/1.0', 200, 2048, '1995-07-01'),
        ('120.170.19.95', 'GET', '/countdown/', 'HTTP/1.0', 200, 0, '1995-07-01'),
        ('120.170.19.95', 'GET', '/countdown/', 'HTTP/1.0', 400, 0, '1995-07-01'),
        ('120.170.19.95', 'GET', '/home/', 'HTTP/1.0', 300, 0, '1995-07-01'),
        ('node3.xx.org', 'GET', '/index/', 'HTTP/1.0', 200, 0, '1995-07-01'),
        ('node3.xx.org', 'GET', '/index/', 'HTTP/1.0', 200, 0, '1995-07-01'),
        ('node3.xx.org', 'GET', '/', 'HTTP/1.0', 200, 0, '1995-07-01'),
    ]

    test_input_df = spark_session.createDataFrame(data=test_input,
                                                  schema=['host', 'method', 'endpoint', 'protocol', 'status',
                                                          'content_size', 'date_str'])
    from pyspark.sql.functions import col
    from pyspark.sql.functions import to_date

    test_input_df = test_input_df.withColumn('date', to_date(col('date_str'), 'yyyy-MM-dd')).drop('date_str')

    # build expected output dataframe
    expected_output = [
        ('1995-07-01', '/shuttle/countdown/', 4),
        ('1995-07-01', '/countdown/', 2),
        ('1995-07-01', '/index/', 2),
        ('1995-07-01', '/', 1),
        ('1995-07-01', '/home/', 1),
    ]
    expect_out_df = spark_session.createDataFrame(data=expected_output,
                                                  schema=['date_str', 'endpoint', 'hits'])
    expect_out_df = expect_out_df.withColumn('date', to_date(col('date_str'), 'yyyy-MM-dd')).drop('date_str')

    print("Expected output dataframe")
    expect_out_df.show(truncate=False)

    actual_output_df = processor.urlsTopNPerDay(test_input_df, 20)

    print("Actual output dataframe")
    actual_output_df.show(truncate=False)

    expect_out_df = get_sorted_data_frame(expect_out_df.toPandas(), expect_out_df.columns)
    actual_output_df = get_sorted_data_frame(actual_output_df.toPandas(), actual_output_df.columns)

    pd.testing.assert_frame_equal(expect_out_df, actual_output_df, check_like=True, check_dtype=False)


def test_fetchTopKHostsPerDay(spark_session):
    '''
    This method test if top k host are fetched from test input dataframe
    :param spark_session:
    :return:
    '''
    test_input = [
        ('d104.aa.net', 'GET', '/shuttle/countdown/', 'HTTP/1.0', 200, 2048, '1995-07-01'),
        ('d104.aa.net', 'GET', '/shuttle/countdown/', 'HTTP/1.0', 200, 512, '1995-07-01'),
        ('d104.aa.net', 'GET', '/shuttle/countdown/', 'HTTP/1.0', 300, 0, '1995-07-01'),
        ('d104.aa.net', 'GET', '/shuttle/countdown/', 'HTTP/1.0', 200, 2048, '1995-07-01'),
        ('120.170.19.95', 'GET', '/shuttle/countdown/', 'HTTP/1.0', 200, 0, '1995-07-01'),
        ('120.170.19.95', 'GET', '/shuttle/countdown/', 'HTTP/1.0', 400, 0, '1995-07-01'),
        ('120.170.19.95', 'GET', '/shuttle/countdown/', 'HTTP/1.0', 300, 0, '1995-07-01'),
        ('node3.xx.org', 'GET', '/shuttle/countdown/', 'HTTP/1.0', 200, 0, '1995-07-01'),
        ('node3.xx.org', 'GET', '/shuttle/countdown/', 'HTTP/1.0', 200, 0, '1995-07-01'),
    ]

    test_input_df = spark_session.createDataFrame(data=test_input,
                                                  schema=['host', 'method', 'endpoint', 'protocol', 'status',
                                                          'content_size', 'date_str'])
    from pyspark.sql.functions import col
    from pyspark.sql.functions import to_date

    test_input_df = test_input_df.withColumn('date', to_date(col('date_str'), 'yyyy-MM-dd')).drop('date_str')

    # build expected output dataframe
    expected_output = [
        ('1995-07-01', 'd104.aa.net', 4),
        ('1995-07-01', '120.170.19.95', 3),
    ]
    expect_out_df = spark_session.createDataFrame(data=expected_output,
                                                  schema=['date_str', 'host', 'hits'])
    expect_out_df = expect_out_df.withColumn('date', to_date(col('date_str'), 'yyyy-MM-dd')).drop('date_str')

    print("Expected output dataframe")
    expect_out_df.show(truncate=False)

    actual_output_df = processor.hostsTopNPerDay(test_input_df, 2)

    print("Actual output dataframe")
    actual_output_df.show(truncate=False)

    expect_out_df = get_sorted_data_frame(expect_out_df.toPandas(), expect_out_df.columns)
    actual_output_df = get_sorted_data_frame(actual_output_df.toPandas(), actual_output_df.columns)

    pd.testing.assert_frame_equal(expect_out_df, actual_output_df, check_like=True, check_dtype=False)


def test_fetchTopKHostsPerDayTieCase(spark_session):
    '''
    This method test if top k hosts by count are fetched from test input dataframe when there
    is tie among the hosts. i.e when count matches.
    :param spark_session:
    :return:
    '''

    test_input = [
        ('d104.aa.net', 'GET', '/shuttle/countdown/', 'HTTP/1.0', 200, 2048, '1995-07-01'),
        ('d104.aa.net', 'GET', '/shuttle/countdown/', 'HTTP/1.0', 200, 512, '1995-07-01'),
        ('d104.aa.net', 'GET', '/shuttle/countdown/', 'HTTP/1.0', 300, 0, '1995-07-01'),
        ('d104.aa.net', 'GET', '/shuttle/countdown/', 'HTTP/1.0', 200, 2048, '1995-07-01'),
        ('120.170.19.95', 'GET', '/countdown/', 'HTTP/1.0', 200, 0, '1995-07-01'),
        ('120.170.19.95', 'GET', '/index/', 'HTTP/1.0', 200, 0, '1995-07-01'),
        ('120.170.19.95', 'GET', '/shuttle/countdown/', 'HTTP/1.0', 400, 0, '1995-07-01'),
        ('120.170.19.95', 'GET', '/shuttle/countdown/', 'HTTP/1.0', 300, 0, '1995-07-01'),
        ('node3.xx.org', 'GET', '/shuttle/countdown/', 'HTTP/1.0', 200, 0, '1995-07-01'),
        ('node3.xx.org', 'GET', '/shuttle/countdown/', 'HTTP/1.0', 200, 0, '1995-07-01'),
        ('ip.xx.org', 'GET', '/shuttle/countdown/', 'HTTP/1.0', 200, 0, '1995-07-01'),
    ]

    test_input_df = spark_session.createDataFrame(data=test_input,
                                                  schema=['host', 'method', 'endpoint', 'protocol', 'status',
                                                          'content_size', 'date_str'])
    from pyspark.sql.functions import col
    from pyspark.sql.functions import to_date

    test_input_df = test_input_df.withColumn('date', to_date(col('date_str'), 'yyyy-MM-dd')).drop('date_str')

    # build expected output dataframe
    expected_output = [
        ('1995-07-01', 'd104.aa.net', 4),
        ('1995-07-01', '120.170.19.95', 4),
        ('1995-07-01', 'node3.xx.org', 2),
    ]
    expect_out_df = spark_session.createDataFrame(data=expected_output,
                                                  schema=['date_str', 'host', 'hits'])
    expect_out_df = expect_out_df.withColumn('date', to_date(col('date_str'), 'yyyy-MM-dd')).drop('date_str')

    print("Expected output dataframe")
    expect_out_df.show(truncate=False)

    actual_output_df = processor.hostsTopNPerDay(test_input_df, 2)

    print("Actual output dataframe")
    actual_output_df.show(truncate=False)

    expect_out_df = get_sorted_data_frame(expect_out_df.toPandas(), expect_out_df.columns)
    actual_output_df = get_sorted_data_frame(actual_output_df.toPandas(), actual_output_df.columns)

    pd.testing.assert_frame_equal(expect_out_df, actual_output_df, check_like=True, check_dtype=False)


def test_fetchTopKHostsPerDayWhenLessRecordsThanK(spark_session):
    '''
    This method test if top k hosts by count are fetched from test input dataframe when there
    is tie among the hosts. i.e when count matches.
    :param spark_session:
    :return:
    '''

    test_input = [
        ('d104.aa.net', 'GET', '/shuttle/countdown/', 'HTTP/1.0', 200, 2048, '1995-07-01'),
        ('d104.aa.net', 'GET', '/shuttle/countdown/', 'HTTP/1.0', 200, 512, '1995-07-01'),
        ('d104.aa.net', 'GET', '/shuttle/countdown/', 'HTTP/1.0', 300, 0, '1995-07-01'),
        ('d104.aa.net', 'GET', '/shuttle/countdown/', 'HTTP/1.0', 200, 2048, '1995-07-01'),
        ('120.170.19.95', 'GET', '/countdown/', 'HTTP/1.0', 200, 0, '1995-07-01'),
        ('120.170.19.95', 'GET', '/index/', 'HTTP/1.0', 200, 0, '1995-07-01'),
        ('120.170.19.95', 'GET', '/shuttle/countdown/', 'HTTP/1.0', 400, 0, '1995-07-01'),
        ('120.170.19.95', 'GET', '/shuttle/countdown/', 'HTTP/1.0', 300, 0, '1995-07-01'),
        ('node3.xx.org', 'GET', '/shuttle/countdown/', 'HTTP/1.0', 200, 0, '1995-07-01'),
        ('node3.xx.org', 'GET', '/shuttle/countdown/', 'HTTP/1.0', 200, 0, '1995-07-01'),
        ('ip.xx.org', 'GET', '/shuttle/countdown/', 'HTTP/1.0', 200, 0, '1995-07-01'),
    ]

    test_input_df = spark_session.createDataFrame(data=test_input,
                                                  schema=['host', 'method', 'endpoint', 'protocol', 'status',
                                                          'content_size', 'date_str'])
    from pyspark.sql.functions import col
    from pyspark.sql.functions import to_date

    test_input_df = test_input_df.withColumn('date', to_date(col('date_str'), 'yyyy-MM-dd')).drop('date_str')

    # build expected output dataframe
    expected_output = [
        ('1995-07-01', 'd104.aa.net', 4),
        ('1995-07-01', '120.170.19.95', 4),
        ('1995-07-01', 'node3.xx.org', 2),
        ('1995-07-01', 'ip.xx.org', 1),
    ]
    expect_out_df = spark_session.createDataFrame(data=expected_output,
                                                  schema=['date_str', 'host', 'hits'])
    expect_out_df = expect_out_df.withColumn('date', to_date(col('date_str'), 'yyyy-MM-dd')).drop('date_str')

    print("Expected output dataframe")
    expect_out_df.show(truncate=False)

    actual_output_df = processor.hostsTopNPerDay(test_input_df, 20)

    print("Actual output dataframe")
    actual_output_df.show(truncate=False)

    expect_out_df = get_sorted_data_frame(expect_out_df.toPandas(), expect_out_df.columns)
    actual_output_df = get_sorted_data_frame(actual_output_df.toPandas(), actual_output_df.columns)

    pd.testing.assert_frame_equal(expect_out_df, actual_output_df, check_like=True, check_dtype=False)


@mock.patch('glob.glob', return_value=['data/NASA_access_log_Jul95.gz'])
def test_downloader_local_files(mock_glob):
    ok, err = processor.downloader(None, 'data')
    assert ok == True
    assert err == ''


@mock.patch('glob.glob', return_value=[])
def test_downloader_local_no_files(mock_glob):
    ok, err = processor.downloader(None, 'data')
    assert ok == False
    assert err != ''


def get_sorted_data_frame(data_frame, columns_list):
    return data_frame.sort_values(columns_list).reset_index(drop=True)
