from os import path , listdir
import os
import pytesseract
from pdf2image import convert_from_path
pytesseract.pytesseract.tesseract_cmd = r"C:\Program Files\Tesseract-OCR\tesseract.exe"
tessdata_dir_config = r'--tessdata-dir "C:\Program Files\Tesseract-OCR\tessdata"'
from datetime import datetime
from pyspark import SparkConf
from pyspark.sql import SparkSession
import pytesseract
from pdf2image import convert_from_path

#Paths :
dirpath = "C:/Users/asus/OneDrive/Documents/text_images"
parent_path = os.path.abspath(os.path.join(dirpath, os.pardir))
INPUT_PATH = "{dirpath}/resources/input_1".format(dirpath=dirpath)
OUTPUT_PATH = "{dirpath}/resources/output_1".format(dirpath=dirpath)
#Pytesseract config :
pytesseract.pytesseract.tesseract_cmd = r"C:\Program Files\Tesseract-OCR\tesseract.exe"
tessdata_dir_config = r'--tessdata-dir "C:\Program Files\Tesseract-OCR\tessdata"'

#functions :
def get_images(pdf_file, n):

    base = path.basename(pdf_file)
    filename = path.splitext(base)[0]
    images = convert_from_path(pdf_file,350) #dpi or resolution = 350

    return [(filename, images[i:i + n], i) for i in range(0, len(images), n)]

def convert_images_to_text(images):
    """
    Convert pdf byte stream to text.
    :param pdf_file: content of pdf file as a byte stream
    :return:
    """
    pages = [pytesseract.pytesseract.image_to_string(image , lang='fra',config=tessdata_dir_config) for image in images[1]]

    return (images[0], pages, images[2])


def merge_string(list_of_tuple):
    return '\n'.join(item[0] for item in list_of_tuple)

def write_to_file(output_folder, filename, content):
    output_filepath = path.join(output_folder, filename + '.txt')
    with open(output_filepath, 'w',encoding='utf8') as f:
        f.write(content)

def save_to_disk(ouput_folder, converted):
    converted.foreach(lambda x: write_to_file(ouput_folder, x[0], x[1]) )

if __name__ == "__main__":

    conf = SparkConf()
    spark = SparkSession \
    .builder \
    .appName("spark-ocr") \
    .master("local") \
    .config(conf=SparkConf()) \
    .getOrCreate()


    start = datetime.now()

    input_files = [
        path.join(INPUT_PATH, f)
        for f in listdir(INPUT_PATH) if f.endswith('.pdf')
    ]

    init_rdd = spark.sparkContext.parallelize([get_images(pdf_file, 1) for pdf_file in input_files],numSlices=10)

    rdd_splitted = init_rdd.flatMap(lambda col: col)

    converted = rdd_splitted.map(lambda ele: convert_images_to_text(ele))

    sorted_list = converted.sortBy(lambda x: (x[0],x[2]))

    merged_list = (
        sorted_list
        .map(lambda nameTuple: (nameTuple[0], [ nameTuple[1] ]))
        .reduceByKey(lambda a, b: a + b)
    )

    # merged_list.foreach(print)

    merged_string = merged_list.mapValues(lambda l: merge_string(l))

    save_to_disk(OUTPUT_PATH, merged_string)


    end = datetime.now()
    print(end - start)