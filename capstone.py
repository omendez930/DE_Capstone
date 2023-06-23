import findspark
findspark.init()

import json
import requests
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat, concat_ws, col, initcap, lower, substring, lit

spark = SparkSession.builder.master("local[*]").appName('CreditCardData').getOrCreate()

spark.stop()