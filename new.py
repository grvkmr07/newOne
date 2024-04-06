from pyspark.sql.session import SparkSession
import pandas as pd
import numpy as np
import os
from pyspark import SparkContext, SparkConf

path = r'/Users/grvkmr07/Documents'
spark = SparkSession.builder.appName("ABC").getOrCreate()
for file in os.listdir(path):
    spk =spark.read.text(os.path.join(path, file))
    print(f"{file}: Total Count: {spk.count()}")

