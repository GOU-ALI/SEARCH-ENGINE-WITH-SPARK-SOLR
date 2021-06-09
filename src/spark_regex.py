import re
from pyspark.sql.functions import  udf
from pyspark.sql.types import StringType
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
import pysolr
import json

#regex fonctions :
def cin(text):
    result = re.search(r'(?<=identité N°.)[A-Z][A-Z0-9][0-9]{5}', text)
    if result!=None:
        return result.group()
    else:
        print('None')

def no_compte(text):
    result =  re.search(r'[0-9]{4}[A-Z][0-9]{9}', text)
    if result!=None:
        return result.group()
    else:
        print('None')


def date_naissance(text) :
    result = re.search(r'\d{2}/\d{2}/\d{4}', text)
    if result!=None:
        return result.group()
    else:
        print('None')

def tel(text) :
    result = re.search(r'06[0-9]{8}', text)
    if result!=None:
        return result.group()
    else:
        print('None')

def name(text):
   tel = re.search('(?<=assurer :)(.*[A-Z\s]?)(?=Pièce)', text)
   if(tel==None):print('None')
   else :return re.match('(|[A-Z\s]+)(.|\s+)([A-Z\s]+)', tel.group()).group()

def File_path(text):
    return text.replace('file:/','').replace('output','input').replace('txt','pdf')

#------------------------------------------------------------------------------------------------------------#
if __name__ == "__main__":
    spark = SparkSession.builder.master("local") \
                        .appName('Spark_regex') \
                        .config(conf=SparkConf()).getOrCreate()

    rdd = spark.sparkContext.wholeTextFiles("C:/Users/asus/OneDrive/Documents/text_images/resources/output")
    list = spark.createDataFrame(rdd).collect()
    df = spark.createDataFrame(list,["file_path", "content"])


    #udfs functions :----------------------------------------------------------------------------------------#
    cin_udf = udf(cin, StringType())
    date_naissance_udf = udf(date_naissance,StringType())
    tel_udf = udf(tel ,StringType())
    name_udf = udf(name,StringType())
    no_compte_udf = udf(no_compte,StringType())
    file_path_udf = udf(File_path,StringType())
#------------------------------------------------------------------------------------------------------------#
    df1 = df.withColumn("No_Compte",no_compte_udf(df.content))
    df2 = df1.withColumn("Full_name",name_udf(df.content))
    df3 = df2.withColumn("CIN", cin_udf(df.content))
    df4 = df3.withColumn("Date_naissance",date_naissance_udf(df.content))
    df5 = df4.withColumn("Telephone",tel_udf(df.content))
    df6 = df5.withColumn("file_path",file_path_udf(df.file_path))
    list2 = df6.drop('content').collect()
    df7 = spark.createDataFrame(list2)
    df8 = df7.withColumn("id", monotonically_increasing_id())
    df8.show(truncate=False)
#--------------------------------------------------------------------------------------------------------------#
"""
#Solr integration :
results = df8.toJSON().map(lambda j: json.loads(j)).collect()
solr = pysolr.Solr('http://127.0.0.1:8983/solr/AWB2',  search_handler='/autocomplete')
#solr.delete(q='*:*')
solr.add(results)
solr.commit()
print("Send to solr successfully :) ")"""
