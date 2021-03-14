import sys

from pyspark.sql import *
from lib.logger import Log4J


if __name__ == "__main__":
    # create spark session
    spark = SparkSession.builder\
        .master("local[3]")\
        .appName("mySparkSchema")\
        .getOrCreate()

    #start the logger
    logger = Log4J(spark)
    logger.info("Starting HelloSpark")

    # #show spark config parameters
    # conf_out = spark.sparkContext.getConf()
    # logger.info(conf_out.toDebugString())

    # # check for the command name argument
    # if len(sys.argv) != 2:
    #     logger.error("Usage: mySparkSchema <filename>")
    #     sys.exit(-1)
    # else:
    #     logger.info("Good: Found command line argument.")

    # read data into apache spark 's dataframe, which is stored in HDFS, which is partitioned to many nodes.
    #file_name = sys.argv[1]
    flight_df = spark.read\
        .format('csv')\
        .option("header", "true")\
        .load("data/flight*.csv")


    flight_df.show(5)
    logger.info("CSV Schema:" + flight_df.schema.simpleString())