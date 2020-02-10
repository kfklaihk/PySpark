KaiOS PySpark Test Run Readme

Author : Kevin Lai

Description
-The Python executable is built and run on Python 3.7 and Spark 2.4.5
-Install Python 3.7 under a directory with no spaces (e.g. D:\ -- OK.  C:\Program Files  -- may cause errors when starting some plugins)
-Install latest pyspark using "pip install pyspark"
-Install Java SE 8 and set environment variable JAVA_HOME to point to the Java directory
-Set environment variables SPARK_HOME and HADOOP_HOME to point to the pyspark directory (e.g. D:\Python37\Lib\site-packages\pyspark in my computer)
-Download and install WinUtils.exe and put it under the bin directory under pyspark directory. This is necessary for writing files from Spark Dataframe

KaiOS Test
-Unzip KaiOS_test.zip to a working directory
-The test data file userid-timestamp-artid-artname-traid-traname.tsv is too big to be included in the zip. Please download and put it in a working directory before running test
-Delete unzipped directories Test_Q1.csv, Test_Q2.csv,Test_Q3.csv and log file KaiOS_test_run.log before running test
-The test output files are written to generated directories Test_Q1.csv, Test_Q2.csv,Test_Q3.csv. The log file KaiOS_test_run.log is generated in the working directory
-Type "python kaios_de_test.py" to run the Data Engineer test
-Test the Engineering Assignment with kaios_eng_test.sql in Postgresql