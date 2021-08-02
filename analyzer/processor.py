"""
Process Top K hosts or Urls from NASA Web Server Log files
"""
import logging
import sys

from pyspark.sql.session import SparkSession

FORMAT = '%(asctime)-15s %(levelname)s %(filename)s %(lineno)d  %(message)s'
logging.basicConfig(level=logging.INFO, format=FORMAT)
logger = logging.getLogger(__name__)


# Download URL and save to outpath.
def downloader(url, outpath):
    '''
    Downaloads the dataset(s) from the given url into the directory specified by outpath
    :param url:  Remote URL for datasets
    :param outpath: Directory into which the datasets are downloaded.
    :return:
    '''

    # From URL construct the destination path and filename.
    import os
    import urllib.request

    if url:
        file_name = os.path.basename(urllib.parse.urlparse(url).path)
        file_path = os.path.join(outpath, file_name)

        from pathlib import Path
        Path(outpath).mkdir(parents=True, exist_ok=True)

        # Check if the file has already been downloaded.
        if os.path.exists(file_path):
            logger.info(('input data file {} already downloaded'.format(file_path)))
            return True, ''

        # Download and write to file.
        logger.info('Downloading dataset using the url {}'.format(url))
        try:
            with urllib.request.urlopen(url, timeout=5) as urldata, \
                    open(file_path, 'wb') as out_file:
                import shutil
                shutil.copyfileobj(urldata, out_file)
        except Exception as e:
            # logging.error(f'Failed to Download Dataset from {url}, {str(e)}'.format(url, e))
            return False, str(e)
    else:
        import glob
        input_data_files = glob.glob(outpath + '/*.gz')
        if not input_data_files:
            return False, 'Dateset(s) do not exist in directory {}'.format(outpath)

    return True, ''


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


def null_values_data_summary(input_df):
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
    logger.info("summary of Null values in dataframe")
    input_df.agg(*exprs).show()


def parse_timestamp(t_string):
    """ Convert Log time  into a Python datetime object
    Args:
        t_string: date and time in Apache time format [dd/mmm/yyyy:hh:mm:ss (+/-)zzzz]
    Returns:
        a string suitable for passing to CAST('timestamp')
    """
    month_map = {
        'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May': 5, 'Jun': 6, 'Jul': 7,
        'Aug': 8, 'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12
    }
    # NOTE: We're ignoring the time zones here, might need to be handled depending on the problem you are solving
    return "{0:04d}-{1:02d}-{2:02d} {3:02d}:{4:02d}:{5:02d}".format(
        int(t_string[7:11]),
        month_map[t_string[3:6]],
        int(t_string[0:2]),
        int(t_string[12:14]),
        int(t_string[15:17]),
        int(t_string[18:20])
    )


def drop_rows_with_nulls_any_column(df):
    '''
    Drops rows that have nulls in any of the columns.

    :param df:
    :return: dataframe
    '''
    logger.info('Discording rows with Null values in all columns in dataframe')
    return df.na.drop()


def replace_empty_string_with_nulls(df):
    '''
    This method replaces empty strings with nulls in whole dataframe.
    :param df:
    :return:  dataframe
    '''
    from pyspark.sql.functions import when, col
    # For all failed or redirection requests, log data file has '-' for content size.
    # regexp_extract() returns empty string if not matched with regex, as content_size
    # is casted to integer, empty string would become Null integer.
    # so, all Nulls in content_size have to be converted to Zeros in the dataframe.
    logger.info('Replacing empty strings with nulls in all dataframe columns')
    return df.select([when(col(c) == "", None).otherwise(col(c)).alias(c) for c in df.columns])


def replace_null_with_value(df, column_name, value=0):
    '''
    This method replaces null values with Zeros for content_size column.

    :param df:
    :param column_name:
    :param value:
    :return: Dataframe after replacing nulls with zeros
    '''
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

    start_cnt = input_df.count()

    # Step#1 Replace nulls with Zeros for content_size column.
    logger.info('Setting Null content sizes to zero')
    input_df = replace_null_with_value(input_df, 'content_size', 0)

    # Step#2 Replace empty strings with Nulls as reg_extract() returns
    # a empty string if regex did not match. RegEx did not match means,
    # it's corrupted column
    logger.info('Replacing empty sting values with Nulls for all columns in dataframe')
    input_df = replace_empty_string_with_nulls(input_df)

    # Print summary of columns with null values in the dataframe.
    logger.info('Replacing empty sting values with Nulls for all columns in dataframe')
    null_values_data_summary(input_df)

    # Skip all rows with null column values from the dataframe
    logger.info('Dropping rows with nulls in any columns in dataframe')
    input_df = drop_rows_with_nulls_any_column(input_df)

    # Now, transform timestamp to Date column. Here, our objective of the analysis is to
    # find the top-k visitors, or/and urls per day, data needs to be grouped by date rather
    # than by timestamp.
    end_cnt = input_df.count()
    logger.info('Record counts: Before transformation {}, '
                'after transformation {} , and discorded count {}'.format(start_cnt, end_cnt, (start_cnt - end_cnt)))

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

    df = input_df.withColumn('host', regexp_extract('value', get_host_pattern(), 1)) \
        .withColumn('time', regexp_extract('value', get_time_stamp_pattern(), 1)) \
        .withColumn('method', regexp_extract('value', get_method_uri_protocol_pattern(), 1)) \
        .withColumn('endpoint', regexp_extract('value', get_method_uri_protocol_pattern(), 2)) \
        .withColumn('protocol', regexp_extract('value', get_method_uri_protocol_pattern(), 3)) \
        .withColumn('status', regexp_extract('value', get_status_pattern(), 1).cast('integer')) \
        .withColumn('content_size',
                    regexp_extract('value', get_content_size_pattern(), 1).cast('integer')).drop('value')
    return df


def ingest(spark_session, target_dir):
    '''
    This method gets the list of *.gz log data files from target directory.
    For this exercise, assume the files were already extracted.

    :return: dataframe with log lines
    '''

    import glob
    input_data_files = glob.glob(target_dir + '/*.gz')
    logger.info('Ingesting Input log data files {}'.format(input_data_files))
    return spark_session.read.text(input_data_files)


tasks = {}
task = lambda f: tasks.setdefault(f.__name__, f)


@task
def urlsTopNPerDay(input_df, top_k):
    '''
    This method queries the cached dataframe and fetches top_k urls on each day. First data is
    grouped by date, urls and counts hits per url on that day. Then, uses dense_rank function
    to rank each url, orders data by rank in descending order. Finally, selects that urls those
    are ranked by top_k param.


    :param input_df: transformed dataframe
    :param top_k: top K frequency. For instance, top 2, 3 or k hosts per day.
    :return: a data frame with [date, hosts, hits] for top k hosts.
    '''
    from pyspark.sql import Window
    from pyspark.sql.functions import col, dense_rank

    df_hosts_by_date = input_df.groupBy('date', 'endpoint').count().sort('date').withColumnRenamed('count', 'hits')
    wind_spec = Window.partitionBy('date').orderBy(col('hits').desc())

    # ranked_df = df_date_hosts.withColumn('rank', row_number().over(windSpec))
    ranked_df = df_hosts_by_date.withColumn('rank', dense_rank().over(wind_spec))
    return ranked_df.filter(col('rank') <= top_k).select('date', 'endpoint', 'hits').orderBy('date', col('hits').desc())


@task
def hostsTopNPerDay(input_df, top_k):
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
    '''
     Gets or Create spark sessions and returns it.
     Assumption is this application runs in local mode inside docker container

    :return: sparkSession
    '''
    # Get Spark session
    logger.info('Get or create Spark Session')
    return SparkSession.builder \
        .master("local[*]") \
        .appName("WebServerLogAnalyzer") \
        .getOrCreate()


def parse_args(args):
    '''
    This method parses the command line agrguments and returns those arguments.
    :param args:
    :return:
    '''
    import argparse
    parser = argparse.ArgumentParser()
    sub_parser = parser.add_subparsers(dest='command')
    hosts = sub_parser.add_parser('hosts')
    urls = sub_parser.add_parser('urls')
    hosts.add_argument('--top', type=int, required=True,
                       help='<hosts --top N > where N indicates top hosts. N integer from 1 and above')
    urls.add_argument('--top', type=int, required=True,
                      help='<urls --top N> where N indicates top urls. N integer from 1 and above')
    parser.add_argument('--dataset', required=False, action='store',
                        help='url to the dataset to be downloaded from '
                             'internet/file system. http:// or ftp:// or file:// ')
    parser.add_argument('-csv', '--csv', required=False, action='store_true',
                        help='Stores the analysis report to data folder in CSV format')
    args_out = parser.parse_args(args)
    print(args_out)
    return args_out


def main():
    '''
    This is main entry point of the code. It parses the command line arguments,
    ingests, extract, transform, runs analytic functions of the transformed
    dataframe, and either prints to the console, or saves the  data in csv format.
    :return: exit code
    '''
    import sys

    args = parse_args(sys.argv[1:])
    top_k = args.top
    if top_k < 1:  # edge case
        logger.error('Top K value is less than 1, top value is 1 and above')
        return 1
    logger.info('Started fetching top {} {} per day from log data files'.format(top_k, args.command))

    # read log data from folder.
    input_data_dir = 'data'
    ok, err = downloader(args.dataset, input_data_dir)
    if not ok:
        logger.error(err)
        return 1

    spark = get_spark_session()
    # ingest
    log_df = ingest(spark, input_data_dir)
    # Extract
    log_df = extract(log_df)

    import time
    start = time.time()
    # transform
    log_df = transform(log_df)
    logger.info('Time taken to transform log dataframe to desired state is {} seconds'
                .format((time.time() - start)))
    # Cache dataframe for further analysis.
    logger.info('Caching the Dataframe for future analytical queries')
    log_df.cache()

    start = time.time()
    # top_k_hosts = hostsTopNPerDay(log_df, top_k)
    top_N_df = tasks[args.command + 'TopNPerDay'](log_df, top_k)
    if args.csv:
        # Now derive the insights
        csv_loc = 'data/top{}{}perday.csv'.format(top_k, args.command)
        logger.info(
            'Storing top {} {} per day data to a CSV file at location {} '.format(top_k, args.command, csv_loc))
        # Assumption is that result data is small enough to be fit in a single partition.
        top_N_df.repartition(1).write.csv(path=csv_loc, mode="overwrite", header="true")
        logger.info('Time taken to fetch top {} {} per day data and writing to csv is {} seconds'
                    .format(top_k, args.command, (time.time() - start)))
    else:
        logger.info('Printing Top {} {} per day data to console'.format(top_k, args.command))
        top_N_df.show(top_N_df.count(), truncate=False)

    logger.info('PySpark job is completed successfully')
    # close spark session
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
