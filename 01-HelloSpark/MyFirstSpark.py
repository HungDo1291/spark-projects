import sys

from pyspark.sql import *
from lib.my_first_logger import Log4J
from lib.utils import get_spark_app_config

if __name__ == "__main__":
    confs = get_spark_app_config()
    spark = SparkSession.builder\
        .config(conf= confs)\
        .getOrCreate()

    logger = Log4J(spark)
    logger.info("Starting HelloSpark")
    #Your procssing code

    #show spark config parameters
    conf_out = spark.sparkContext.getConf()
    logger.info(conf_out.toDebugString())


    # check for the command name argument
    if len(sys.argv) != 2:
        logger.error("Usage: HelloSpark <filename>")
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

    #simulate spark partition:
    partitioned_survey_df = survey_df.repartition(2)

    filtered_survey_df = partitioned_survey_df.where("Age < 40")\
        .select("Age", "Gender", "Country", "state")\
        .groupBy("Country")\
        .count()

    filtered_survey_df.show()
    logger.info(filtered_survey_df.collect())

    input("Press Enter")  # to pause the program
    logger.info("Finished HelloSpark")
    #spark.stop()