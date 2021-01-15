import sys
sys.path.insert(0, '.')
from pyspark import SparkContext, SparkConf
import re

class Utils():
    COMMA_DELIMITER = re.compile(''',(?=(?:[^"]*"[^"]*")*[^"]*$)''')


def splitComma(line: str):
    splits = Utils.COMMA_DELIMITER.split(line)
    return "{}, {}".format(splits[1], splits[6])

if __name__ == "__main__":
    conf = SparkConf().setAppName("airports").setMaster("local[*]")
    sc = SparkContext(conf = conf)

    airports = sc.textFile("in/airports.text")
    airportsByLatitude = airports.filter(lambda line : float(Utils.COMMA_DELIMITER.split(line)[6]) > 40)

    airportsNameAndLat = airportsByLatitude.map(splitComma)
    airportsNameAndLat.saveAsTextFile("out/airports_by_latitude.text")
