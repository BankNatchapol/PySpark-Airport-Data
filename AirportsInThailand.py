import sys
sys.path.insert(0, '.')
from pyspark import SparkContext, SparkConf
import re

class Utils():
    COMMA_DELIMITER = re.compile(''',(?=(?:[^"]*"[^"]*")*[^"]*$)''')

def splitComma(line: str):

    splits = Utils.COMMA_DELIMITER.split(line)
    return "{}, {}".format(splits[1], splits[2])

if __name__ == "__main__":
    conf = SparkConf().setAppName("airports").setMaster("local[*]")
    sc = SparkContext(conf = conf)

    airports = sc.textFile("in/airports.text")
    airportsInTH = airports.filter(lambda line : Utils.COMMA_DELIMITER.split(line)[3] == "\"Thailand\"")

    airportsNameAndCityNames = airportsInTH.map(splitComma)
    airportsNameAndCityNames.saveAsTextFile("out/airports_in_thailand.text")
