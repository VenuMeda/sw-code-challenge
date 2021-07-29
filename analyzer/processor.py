"""
Process Top K hosts or Urls from NASA Web Server Log files
"""

import time
import logging

from pyspark.sql.session import SparkSession

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
        return spark_sum(col(col_name).isNull().cast('integer')).alias(
            col_name)

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


def drop_rows_with_nulls_any_column(df):
    from pyspark.sql.functions import col, when
    logger.info('Discording rows with Null values in all columns in dataframe')
    return df.na.drop()


def replace_empty_string_with_nulls(df):
    from pyspark.sql.functions import when, col
    # For all failed or redirection requests, log data file has '-' for content size.
    # so, let's convert all Nulls to 0 bytes size in the dataframe.
    logger.info('Replacing empty strings with nulls in all dataframe columns')
    return df.select([when(col(c) == "", None).otherwise(col(c)).alias(c) for c in df.columns])


def replace_null_with_value(df, column_name, value=0):
    # For all failed or redirection requests, log data file has '-' for content size.
    # so, let's convert all Nulls to 0 bytes size in the dataframe.
    logger.info('Replacing Null values with 0 for content_size column')
    return df.na.fill({column_name: value})


def transform_timestamp_day(input_df):
    '''
    This method takes input data frame transform the timestamp column to date and drops
    the timestamp column. An udf was used to transform timestamp string to Date.
    :param input_df:
    :return: returns dataframe with date column.
    '''
    from pyspark.sql.functions import udf, to_date

    logger.info('Converting timestamp to date format using  to_date() function on DataFrame')
    udf_parse_time = udf(parse_timestamp)

    # select all columns and while parsing the timestamp to time and drop the original timestamp.
    input_df = input_df.withColumn('timestamp', udf_parse_time(input_df['time']).cast('timestamp')).drop('time')

    return input_df.withColumn('date', to_date('timestamp')).drop('timestamp')


def transform(input_df):
    '''
    This method takes input dataframe and parses each line of log data file,
    extract data into host, date, method, endpoint, protocol, status, and content_size columns
    regex.

    :param input_df: input data frame contains single column with each log entry as row.
    :return: result parsed dataframe after extracting the individual
    columns [host, date, method, endpoint, protocol, status, and content_size]
    '''

    # Step#1 Replace nulls with Zeros for content_size column.
    input_df = replace_null_with_value(input_df, 'content_size', 0)

    # Step#2 Replace empty strings with Nulls as reg_extract() returns
    # a empty string if regex did not match. RegEx did not match means,
    # it's corrupted column
    input_df = replace_empty_string_with_nulls(input_df)

    # Print summary of columns with null values in the dataframe.
    check_for_nulls_print_summary(input_df)

    # Skip all rows with null column values from the dataframe
    input_df = drop_rows_with_nulls_any_column(input_df)

    # Now, transform timestamp to Date column. Here, our objective of the analysis is to
    # find the top-k visitors, or/and urls per day, data needs to be grouped by date rather
    # than by timestamp.

    return transform_timestamp_day(input_df)


def extract(input_df):
    """
    This method takes input dataframe and parses single column dataframe to new dataframe with
    [host,timestamp, method, endpoint, protocol, status, and content_size] columns using regex.
    If the regex did not match, or the specified group did not match, Null is used.

    :rtype: Spark Dataframe.
    """
    from pyspark.sql.functions import regexp_extract
    logger.info('Extracting raw log entries to rows and columns in Dataframe using regex')

    return input_df.withColumn('host', regexp_extract('value', get_host_pattern(), 1)) \
        .withColumn('time', regexp_extract('value', get_time_stamp_pattern(), 1)) \
        .withColumn('method', regexp_extract('value', get_method_uri_protocol_pattern(), 1)) \
        .withColumn('endpoint', regexp_extract('value', get_method_uri_protocol_pattern(), 2)) \
        .withColumn('protocol', regexp_extract('value', get_method_uri_protocol_pattern(), 3)) \
        .withColumn('status', regexp_extract('value', get_status_pattern(), 1).cast('integer')) \
        .withColumn('content_size',
                    regexp_extract('value', get_content_size_pattern(), 1).cast('integer')).drop('value')


def ingest(spark_session, target_dir):
    '''
    This method gets the list of *.gz log data files from target directory.
    TODO: Spark can directly extract the files from remote ftp:// servers.
    This method can be further improved to ingest data dynamically from ftp severs
    using a list of URLs and target directory.

    For this exercise, assume the files were already extracted.

    :return: list .gz files
    '''

    # For this excerise, assumed the data file
    # data_file = 'ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz'
    # spark.sparkContext.addFile(data_file)
    # df_data_path = SparkFiles.get('NASA_access_log_Jul95.gz')

    import glob
    input_data_files = glob.glob(target_dir + '/*.gz')
    return spark_session.read.text(input_data_files)


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

    if top_k < 1:
        logger.error('Top K value is less than 1, top value is 1 and above')
        exit(1)

    logger.info('Started fetching top {} {} per day from log data files'.format(top_k, args.command))
    spark = get_spark_session()

    # read log data from folder data.
    input_data_dir = 'data'
    input_files = ingest(spark, input_data_dir)

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
    # close spark session
    # spark.close()
