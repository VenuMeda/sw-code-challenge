"""
Process Top K hosts or Urls from NASA Web Server Log files
"""
import time

#
# class Timer(object):
#     '''
#     This is a class based context manager to measure execution
#     time of python code. This is inspired from blog
#     https://blog.usejournal.com/how-to-create-your-own-timing-context-manager-in-python-a0e944b48cf8
#     '''
#
#     def __init__(self, description):
#         self.description = description
#
#     def __enter__(self):
#         self.start = time()
#
#     def __exit__(self, type, value, traceback):
#         self.end = time()
#         print(f"{self.description}: {self.end - self.start}")
#

from pyspark.sql.session import SparkSession

import logging

FORMAT = '%(asctime)-15s %(levelname)s %(filename)s %(lineno)d  %(message)s'
logging.basicConfig(level=logging.INFO, format=FORMAT)
logger = logging.getLogger(__name__)


def get_host_pattern():
    '''

    :return: returns regex string to extract the host from log line
    '''
    return r'(^\S+\.[\S+\.]+\S+)\s'


def get_time_stamp_pattern():
    '''

    :return: regex string to  extract the timestamp from log line
    '''
    return r'\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]'


def get_method_uri_protocol_pattern():
    '''

    :return: regex string to extract the method, URI, and Protocol from log line
    '''
    return r'\"(\S+)\s(\S+)\s*(\S*)\"'


def get_status_pattern():
    '''

    :return:  regex string to extract status from log line
    '''
    return r'\s(\d{3})\s'


def get_content_size_pattern():
    '''

    :return: regex string to extract content size from log line.
    '''
    return r'\s(\d+)$'


def check_for_nulls_print_summary(input_df):
    '''
    This method just prints summary of nulls data in each column.
    :param input_df:
    :return: None
    '''
    from pyspark.sql.functions import col, sum as spark_sum

    def count_null(col_name):
        return spark_sum(col(col_name).isNull().cast('integer')).alias(col_name)

    # Build up a list of column expressions, one per column.
    exprs = [count_null(col_name) for col_name in input_df.columns]

    # Run the aggregation. The *exprs converts the list of expressions into variable function arguments.
    logger.info("Nulls data summary")
    input_df.agg(*exprs).show()


def parse_timestamp(text):
    """ Convert Common Log time format into a Python datetime object
    Args:
        text (str): date and time in Apache time format [dd/mmm/yyyy:hh:mm:ss (+/-)zzzz]
    Returns:
        a string suitable for passing to CAST('timestamp')
    """
    month_map = {
        'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May': 5, 'Jun': 6, 'Jul': 7,
        'Aug': 8, 'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12
    }
    # NOTE: We're ignoring the time zones here, might need to be handled depending on the problem you are solving
    return "{0:04d}-{1:02d}-{2:02d} {3:02d}:{4:02d}:{5:02d}".format(
        int(text[7:11]),
        month_map[text[3:6]],
        int(text[0:2]),
        int(text[12:14]),
        int(text[15:17]),
        int(text[18:20])
    )


def transform_timestamp_day(input_df):
    '''
    This method takes input data frame transform the timestamp column to date and drops
    the timestamp column. An udf was used to transform timestamp string to Date.
    :param input_df:
    :return: returns dataframe with date column.
    '''
    from pyspark.sql.functions import udf, to_date

    udf_parse_time = udf(parse_timestamp)

    # select all columns and while parsing the timestamp to time and drop the original timestamp.
    tmp_df = input_df.select('*',
                             udf_parse_time(input_df['timestamp']).cast('timestamp').alias('time')
                             ).drop('timestamp')
    return tmp_df.select('*', to_date('time').alias('date')).drop('time')


def transform(input_df):
    '''
    This method takes input dataframe and parses each line of log data file,
    extract data into host, date, method, endpoint, protocol, status, and content_size columns
    regex. If the regex did not match, or the specified group did not match, Null is used.

    :param input_df: input data frame contains single column with each log entry as row.
    :return: result parsed dataframe after extracting the individual
    columns [host, date, method, endpoint, protocol, status, and content_size]
    '''
    from pyspark.sql.functions import regexp_extract
    logger.info('Transforming raw log entries to rows and columns in Dataframe using regex')
    df = input_df.select(regexp_extract('value', get_host_pattern(), 1).alias('host'),
                         regexp_extract('value', get_time_stamp_pattern(), 1).alias('timestamp'),
                         regexp_extract('value', get_method_uri_protocol_pattern(), 1).alias('method'),
                         regexp_extract('value', get_method_uri_protocol_pattern(), 2).alias('endpoint'),
                         regexp_extract('value', get_method_uri_protocol_pattern(), 3).alias('protocol'),
                         regexp_extract('value', get_status_pattern(), 1).cast('integer').alias('status'),
                         regexp_extract('value', get_content_size_pattern(), 1).cast('integer').alias('content_size'))

    # check if there are any nulls records such as empty  fields
    # check_for_nulls_print_summary(df)

    # Step#1 clean status  column and skip rows that have null status.
    # There are some nulls in status column as input raw log data file
    # contains an invalid string in status field. Let's filter this out
    # from final dataframe
    logger.info('Removing Null values from status column')
    df = df[df['status'].isNotNull()]

    # Step#2 clean 'content_size'  column and set content size as 0 when it is null.
    # For all failed or redirection requests, log data file has '-' for content size.
    # so, let's convert all Nulls to 0 bytes size in the dataframe.
    logger.info('Replacing Null values with 0 for content_size column')
    df = df.na.fill({'content_size': 0})

    # Now, check again for null data in the transformed dataframe.
    # check_for_nulls_print_summary(df)

    # Now, transform timestamp to Date column. Here, our objective of the analysis is to
    # find the top-k visitors, or/and urls per day, data needs to be grouped by date rather
    # than by timestamp.
    logger.info('Converting timestamp to date format using  to_date() function on DataFrame')
    df = transform_timestamp_day(df)
    return df


def extract(spark_session):
    """
    This method extracts the dataframe from the input log data files.
    The returned dataframe contains one column.

    :rtype: Spark Dataframe from the input log files.
    """
    # create data from raw log data
    return spark_session.read.text(input_files)


def ingest(target_dir):
    '''
    This method gets the list of *.gz log data files from 'data' folder. PySpark,
    This method can be further improved to ingest data dynamically from ftp severs
    using a list of URLs and target directory.

    For this excercise, assume the files were already extracted.

    :return: list .gz files
    '''

    import glob
    return glob.glob(target_dir + '/*.gz')


def fetchTopKUrlsPerDay(input_df, top_k, only_2xx_code=False):
    '''
    This method queries the cached dataframe and fetches top_k urls on each day. First data is
    grouped by date, urls and counts hits per url on that day. Then, uses dense_rank function
    to rank each url, orders data by rank in descending order. Finally, selects that urls those
    are ranked by top_k param.


    :param input_df: transformed dataframe
    :param top_k: top K frequency. For instance, top 2, 3 or k hosts per day.
    :param only_2xx_code: Boolean if True, fetches urls that resulted in 200 OK responses only.
    By default, it is False.
    :return: a data frame with [date, hosts, hits] for top k hosts.
    '''
    from pyspark.sql import Window
    from pyspark.sql.functions import col, dense_rank

    df_hosts_by_date = input_df.groupBy('date', 'endpoint').count().sort('date').withColumnRenamed('count', 'hits')
    wind_spec = Window.partitionBy('date').orderBy(col('hits').desc())

    # ranked_df = df_date_hosts.withColumn('rank', row_number().over(windSpec))
    ranked_df = df_hosts_by_date.withColumn('rank', dense_rank().over(wind_spec))
    return ranked_df.filter(col('rank') <= top_k).select('date', 'endpoint', 'hits').orderBy('date', col('hits').desc())


def fetchTopKHostsPerDay(input_df, top_k):
    '''
    This method queries the cached dataframe and fetches top_k hosts on each day. First data is
    grouped by date, hosts and counts hits per host on that day. Then, uses dense_rank function
    to rank each host, orders data by rank in descenging order. Finally, selects that hosts those
    are ranked by top_k param.

    :param input_df: transformed dataframe
    :param top_k: top K frequency. For instance, top 2, 3 or k hosts per day.
    :return: retuns a data frame with [date, hosts, hits] for top k hosts.
    '''

    from pyspark.sql import Window
    from pyspark.sql.functions import col, dense_rank

    df_hosts_by_date = input_df.groupBy('date', 'host').count().sort('date').withColumnRenamed('count', 'hits')
    wind_spec = Window.partitionBy('date').orderBy(col('hits').desc())

    # ranked_df = df_date_hosts.withColumn('rank', row_number().over(windSpec))
    ranked_df = df_hosts_by_date.withColumn('rank', dense_rank().over(wind_spec))
    return ranked_df.filter(col('rank') <= top_k).select('date', 'host', 'hits').orderBy('date', col('hits').desc())


def get_spark_session():
    # Get Spark session
    logger.info('Get or create Spark Session')
    return SparkSession.builder \
        .master("local[*]") \
        .appName("WebServerLogAnalyzer") \
        .getOrCreate()


if __name__ == "__main__":

    import argparse

    parser = argparse.ArgumentParser()
    sub_parser = parser.add_subparsers(dest='command')
    hosts = sub_parser.add_parser('hosts')
    urls = sub_parser.add_parser('urls')

    hosts.add_argument('--top', type=int, required=True)
    urls.add_argument('--top', type=int, required=True)

    args = parser.parse_args()
    top_k = args.top

    logger.info('Started fetching top {} {} per day from log data files'.format(top_k, args.command))
    spark = get_spark_session()

    # read log data from folder data.
    input_files = ingest('data')

    # Extract, Transform, and Analyze.
    logger.info('Extracting Input log data files {}'.format(input_files))
    log_df = extract(spark)

    start = time.time()
    log_df = transform(log_df)
    logger.info('Input log dataframe was transformed to desired state')
    logger.info('Time taken to transform log dataframe is {} seconds'
                .format((time.time() - start)))

    # Let's cache dataframe after all transformation for further analysis.
    logger.info('Caching the Dataframe for future analytical queries')
    log_df.cache()

    # Now derive the insights
    csv_loc = 'data/top{}{}perday.csv'.format(top_k, args.command)
    if args.command == 'hosts':
        start = time.time()
        top_k_hosts = fetchTopKHostsPerDay(log_df, top_k)
        logger.info('Storing top {} {} per day data to a CSV file at location {} '.format(top_k, args.command, csv_loc))
        top_k_hosts.repartition(1).write.csv(path=csv_loc, mode="overwrite", header="true")
        logger.info('Time taken to fetch top {} {} per day data and writing to csv is {} seconds'
                    .format(top_k, args.command, (time.time() - start)))
        # logger.info('Printing Top {} {} per day data to console'.format(top_k, args.command))
        # top_k_hosts.show(top_k_hosts.count())

    if args.command == 'urls':
        start = time.time()
        top_k_urls = fetchTopKUrlsPerDay(log_df, top_k)
        logger.info('Storing top {} {} per day data to a CSV file at location {} '.format(top_k, args.command, csv_loc))
        top_k_urls.repartition(1).write.csv(path=csv_loc, mode="overwrite", header="true")
        logger.info('Time taken to fetch top {} {} per day data and writing to csv is {} seconds'
                    .format(top_k, args.command, (time.time() - start)))
        # logger.info('Printing Top {} {} per day data to console'.format(top_k, args.command))
        # top_k_urls.show(top_k_urls.count())

    logger.info('PySpark job is completed successfully')