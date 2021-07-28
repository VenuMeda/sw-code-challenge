import pytest

from . import processor

# this allows using the fixture in all tests in this module
pytestmark = pytest.mark.usefixtures("spark_session")


# # def test_do_word_counts(spark_context):
# def test_do_word_counts(spark_session):
#     """ test that a single event is parsed correctly
#     Args:
#         spark_session: test fixture SparkContext
#     """
#
#     test_input = [
#         ' hello spark ',
#         ' hello again spark spark'
#     ]
#
#     # input_rdd = spark_context.parallelize(test_input, 1)
#     input_rdd = spark_session.sparkContext.parallelize(test_input, 1)
#     results = processor.do_word_counts(input_rdd)
#
#     expected_results = {'hello': 2, 'spark': 3, 'again': 1}
#     assert results == expected_results


def test_get_input_data_files_list():
    """ test that a if log data files exists
    """
    expected = ['data/NASA_access_log_Jul95.gz']
    actual = processor.ingest('data')
    assert expected == actual


def test_parse_logs(spark_session):
    """ test that a set of log entries are parsed correctly
        Args:
            spark_session: test fixture SparkContext
        """

    # test_input = [
    #     '199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0" 200 6245',
    #     'unicomp6.unicomp.net - - [01/Jul/1995:00:00:06 -0400] "GET /shuttle/countdown/ HTTP/1.0" 200 3985',
    #     'burger.letters.com - - [01/Jul/1995:00:00:12 -0400] "GET /images/NASA-logosmall.gif HTTP/1.0" 304 0',
    #     'd104.aa.net - - [01/Jul/1995:00:00:13 -0400] "GET /shuttle/countdown/ HTTP/1.0" 404 -'
    # ]
    test_input = 'data/sample_data.txt'
    input_df = spark_session.read.text(test_input)
    actual = processor.transform(input_df)
    assert len(actual.columns) == 7
    assert input_df.count() == actual.count()
