import sys
from pyspark import SparkConf
from collections import namedtuple
from pyspark.sql import SparkSession#, SparkContext
from lib.logger import Log4j

# create schema. Can also define a class and use it to define the schema. However name tuple is more convenient
SurveyRecord = namedtuple("SurveyRecord", ["Age", "Gender", "Country", "State"])
if __name__ == "__main__":
    conf = SparkConf()\
        .setMaster('local[3]')\
        .setAppName('MyFirstRDD')

    #spark context to create the RDD, without SparkSession
    #context = SparkContext(conf=conf)

    # better use SparkContext, which is a higher level object, from which we
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    context = spark.sparkContext

    # set up logger
    logger = Log4j(spark)

    # check cmd argument
    if len(sys.argv) != 2:
        logger.error("Usage: my_RDD <filename>")
        sys.exit(-1)
    #create RDD
    linesRDD = context.textFile(sys.argv[1])
    # RDD basic transformation
    partitionedRDD = linesRDD.repartition(2)
    colsRDD = partitionedRDD.map(lambda line: line.replace('"','').split(",")) # separate col from each line and map to a list
    selectRDD = colsRDD.map(lambda cols: SurveyRecord(int(cols[1]), cols[2], cols[3],cols[4]))
    filteredRDD = selectRDD.filter(lambda r: r.Age<40)
    KeyValueRDD = filteredRDD.map(lambda r: (r.Country, 1)) #map
    countRDD = KeyValueRDD.reduceByKey(lambda v1, v2: v1 +v2) #reduce

    colsList = countRDD.collect()
    for x in colsList:
        logger.info(x)