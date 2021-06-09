Requirements For PySpark

Java Environment (JDK or JSE)
Python (3 or 2)
Spark-(3.1.2-bin-hadoop3.2)
For Windows you should initialize the System Variables and Paths of 
(SPARK_HOME,HADOOP_HOME and JAVA_HOME)

Installing Requirements
-All the Libraries except Pyspark has written to file named requirements.txt
-Just run the following command: "pip install -r requirements.txt"


Submitting the Job
The application takes 4 arguments (two argument for countries).Since example countries are "Netherlands" or "United Kingdom" it takes 
2 country for "or" operation. Remaning two paramether are the names of "csv" files.

-spark-submit challenge.py "Netherlands" "United Kingdom" "dataset_one.csv" "dataset_two.csv"

Testing
-For all 3 create generic functions I used Testing which also includes chispa functions
-I have created another script for tests (challenge_test.py)
-If you just type "pytest" from the project folder it will run all test functions.


Log and Log Rotating
-I have add following code to app for creating writing log into file.
-I used (log4j) for logging in Pyspark

"log4jLogger = sc._jvm.org.apache.log4j
log = log4jLogger.LogManager.getLogger(__name__)"

Log Rotating
-I have intiliase the script in \spark\conf\log4j.properties
-Added following code for Rotating and getting output "\proje\log\ 


# set the log level and name the root logger
# Available Levels: DEBUG, INFO, WARN, ERROR, FATAL
log4j.rootLogger=INFO, ROOT,console
# set the root logger class
log4j.appender.ROOT=org.apache.log4j.RollingFileAppender
# set the name/location of the log file to rotate
log4j.appender.ROOT.File=\\C:\\Users\\zafer\\Desktop\\abn_amro\\proje\\log\\log.txt
# set the max file size before a new file (and backups) are made
log4j.appender.ROOT.MaxFileSize=10KB
# set how many iterations of the log file to keep before deleting old logs
log4j.appender.ROOT.MaxBackupIndex=5
# set log text formatting
log4j.appender.ROOT.layout=org.apache.log4j.PatternLayout
log4j.appender.ROOT.layout.ConversionPattern=%p %t %c - %m%n

-This two line for Rotating with file size
-It creates Max 5 file with 10KB size

log4j.appender.ROOT.MaxFileSize=10KB
# set how many iterations of the log file to keep before deleting old logs
log4j.appender.ROOT.MaxBackupIndex=5

reStructuredText (reST) 
-All functions has docstring comments for informations.
