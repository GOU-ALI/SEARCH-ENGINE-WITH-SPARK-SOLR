from pyspark.sql.functions import  udf
from pyspark.sql.types import StringType
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
import pysolr
import json
import locationtagger



def extract_country(text):

    entities = locationtagger.find_locations(text = text)
    return entities.countries[0]


def File_path(text):
    return text.replace('file:/','').replace('output','input').replace('txt','pdf')

#------------------------------------------------------------------------------------------------------------#
if __name__ == "__main__":
    spark = SparkSession.builder.master("local") \
                        .appName('Spark_nlp') \
                        .config(conf=SparkConf()).getOrCreate()

    rdd = spark.sparkContext.wholeTextFiles("C:/Users/asus/OneDrive/Documents/text_images/resources/output_1")
    list = spark.createDataFrame(rdd).collect()
    df = spark.createDataFrame(list,["file_path", "content"])


    #udfs functions :----------------------------------------------------------------------------------------#

    file_path_udf = udf(File_path,StringType())
    pays_udf = udf(extract_country,StringType())

#------------------------------------------------------------------------------------------------------------#

    df1 = df.withColumn("file_path",file_path_udf(df.file_path))
    df2 = df1.withColumn("Pays",pays_udf(df.content))

    list2 = df2.drop('content').collect()
    df3 = spark.createDataFrame(list2)
    df4 = df3.withColumn("id", monotonically_increasing_id())
    df4.show(truncate=False)


results = df4.toJSON().map(lambda j: json.loads(j)).collect()

solr = pysolr.Solr('http://127.0.0.1:8983/solr/AWB2',  search_handler='/autocomplete')
#solr.delete(q='*:*')
solr.add(results)
solr.commit()
print("Send to solr successfully :) ")