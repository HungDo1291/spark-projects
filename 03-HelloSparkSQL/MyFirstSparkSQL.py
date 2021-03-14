import sys

from pyspark.sql import *
from lib.logger import Log4J


if __name__ == "__main__":

    spark = SparkSession.builder\
        .master("local[3]")\
        .appName("HelloSparkSQL")\
        .getOrCreate()

    logger = Log4J(spark)
    logger.info("Starting HelloSpark")
    #Your procssing code

    #show spark config parameters
    conf_out = spark.sparkContext.getConf()
    logger.info(conf_out.toDebugString())

    # check for the command name argument
    if len(sys.argv) != 2:
        logger.error("Usage: MyFristSparkSQL <filename>")
        sys.exit(-1)
    else:
        logger.info("Good: Found command line argument.")

    # read data into apache spark 's dataframe, which is stored in HDFS, which is partitioned to many nodes.
    file_name = sys.argv[1]
    survey_df = spark.read\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .csv(file_name)

    survey_df.show()
    # spark allows us to register df as a view
    survey_df.createOrReplaceTempView("survey_tbl")
    count_df = spark.sql("select Country, count(1) as count from survey_tbl where Age<40 group by Country")
    count_df.show()