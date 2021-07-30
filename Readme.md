# Overview

Nasa Web Server Log Analyzer is a python based pyspark Application developed for SecureWorks Coding Challenge. This app
parses webserver logs, cleans, and builds, caches a dataframe for further analysis. Goal is to

- Fetch top K visitors(hosts) per each day
- Fetch top K urls per each day

### About Code Structure

Input ingestion URL - ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz. Here, I assume that input files are already
extracted from remote ftp server to local directory "data" for this exercise. This data directory is mounted to docker
container when running the container.

*analyzer/processor.py* This is main entry point. SparkSession object is built here.

*analyzer/processor_test.py* This file contains tests

*analyzer/conftest.py* contains the required test fixtures for SparkSession and SparkContext.

### Assumptions/Observations about the input Data.

1. Data is structured in the following format-
    * `<visitor> - - [timestamp] "<method> <url> <protocol>" <resonseCode> <content_size>`
      <br/> For instance

```burger.letters.com - - [01/Jul/1995:00:00:12 -0400] "GET /images/NASA-logosmall.gif HTTP/1.0" 304 0```
```burger.letters.com - - [01/Jul/1995:00:00:12 -0400] "GET /images/NASA-logosmall.gif HTTP/1.0" 404 -```

Python regex patterns were used to derive columns from raw log data rows.

2. To evaluate top K Visitors, no special logic has been added to filter log-lines based on HTTP method. The current
   implementation considers all requests from all host irrespective of response codes. Irrespective of response code
   200, 3xx, or 4xx, the host name is considered towards the count of hits from the host. Similar case with URLs too.

3. Input log data is available in Docker container in path '/app/data'

### Software versions

	- PySpark/Spark version- 3.0.2
	- IDE- PyCharm, Docker

### Steps to run

1. Clone the project from git repo

2. Build the docker container using  ``` $ docker build -t <image-tag>:<version-tag> <dir>```
   <br/> For instance ``` $ docker build -t serverlog-analyzer:latest ./ ```

3. This analyzer can download the dataset directly from the
   url ``` ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz```
   store it in ```data``` folder on the container and run the analysis. Use ```--dataset <url>``` flag. If this dataset
   url is not provided, mount the ```data``` folder in the current directory to the docker container. A copy of the data
   was already provided in the ```data``` folder. <br/>
   ``` $ docker run --rm -v $(pwd)/data:/app/data serverlog-analyzer:latest --dataset ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz --csv urls --top 3```

4. Run the docker container using ``` $ docker run --rm -v $(pwd)/data:/app/data <docker image> urls --top <k>```
   For instance, to fetch top 5 hosts per each day, run the below command
   <br/>``` $ docker run --rm -v $(pwd)/data:/app/data serverlog-analyzer:latest hosts --top 5```
   To fetch top 3 URLS per each day, run the command. Report will be printed to console.
   <br/>``` $ docker run --rm -v $(pwd)/data:/app/data serverlog-analyzer:latest urls --top 3```

    <br/> 
   -  Make sure  server log data files are available in local directory. I kept the given 
     input file NASA_access_log_Jul95.gz in 'data' folder. 

5. If ```-csv``` or ```--csv``` is given, report will stored to the ```data``` folder in CSV format.
   ``` $ docker run --rm -v $(pwd)/data:/app/data serverlog-analyzer:latest --csv urls --top 3```

### Step to run unit test using pytest.

"pytest" framework was used for testing. It provides great support for fixtures, reusable fixtures, parametrization in
fixtures.

1. To run the pytest , run the docker container with below command.
   <br/> ```$ docker run --rm --entrypoint pytest serverlog-analyzer:latest -s ```
   <br/> or to run in silent mode <br/> ```$ docker run --rm --entrypoint pytest serverlog-analyzer:latest```
   