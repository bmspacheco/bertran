import boto3
import requests
from io import StringIO
import sys
import pandas as pd
import pyspark.sql.functions as F
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from awsglue.dynamicframe import DynamicFrame
from awsglue.transforms import ApplyMapping
from pyspark.sql.functions import col, date_format
from pyspark.sql.types import DateType
from pyspark.context import SparkContext

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
s3_client = boto3.client('s3')
job.init(args['JOB_NAME'], args)

git_csv_url1 = "https://raw.githubusercontent.com/bmspacheco/bertran/main/datos/Clientes.csv"
git_csv_url2 = "https://raw.githubusercontent.com/bmspacheco/bertran/main/datos/Locaciones.csv"
git_csv_url3 = "https://raw.githubusercontent.com/bmspacheco/bertran/main/datos/Productos.csv"
git_csv_url4 = "https://raw.githubusercontent.com/bmspacheco/bertran/main/datos/PedidoCabeceraLima.csv"
git_csv_url5 = "https://raw.githubusercontent.com/bmspacheco/bertran/main/datos/PedidoDetalleLima.csv"
git_csv_url6 = "https://raw.githubusercontent.com/bmspacheco/bertran/main/datos/PedidoCabeceraProvincias.csv"
git_csv_url7 = "https://raw.githubusercontent.com/bmspacheco/bertran/main/datos/PedidoDetalleProvincias.csv"

s3_bucket_name = "data-lake33-utec"

s3_csv_key1 = "raw/Tablas_Generales/Clientes.csv"
response = requests.get(git_csv_url1)
csv_data = response.text
df = pd.read_csv(StringIO(csv_data))
s3_client = boto3.client('s3')
s3_client.put_object(Bucket=s3_bucket_name, Key=s3_csv_key1, Body=csv_data)

s3_csv_key2 = "raw/Tablas_Generales/Locaciones.csv"
response = requests.get(git_csv_url2)
csv_data = response.text
df = pd.read_csv(StringIO(csv_data))
s3_client = boto3.client('s3')
s3_client.put_object(Bucket=s3_bucket_name, Key=s3_csv_key2, Body=csv_data)

s3_csv_key3 = "raw/Tablas_Generales/Productos.csv"
response = requests.get(git_csv_url3)
csv_data = response.text
df = pd.read_csv(StringIO(csv_data))
s3_client = boto3.client('s3')
s3_client.put_object(Bucket=s3_bucket_name, Key=s3_csv_key3, Body=csv_data)

s3_csv_key4 = "raw/Ventas_Lima/PedidoCabeceraLima.csv"
response = requests.get(git_csv_url4)
csv_data = response.text
df = pd.read_csv(StringIO(csv_data))
s3_client = boto3.client('s3')
s3_client.put_object(Bucket=s3_bucket_name, Key=s3_csv_key4, Body=csv_data)

s3_csv_key5 = "raw/Ventas_Lima/PedidoDetalleLima.csv"
response = requests.get(git_csv_url5)
csv_data = response.text
df = pd.read_csv(StringIO(csv_data))
s3_client = boto3.client('s3')
s3_client.put_object(Bucket=s3_bucket_name, Key=s3_csv_key5, Body=csv_data)

s3_csv_key6 = "raw/Ventas_Provincias/PedidoCabeceraProvincias.csv"
response = requests.get(git_csv_url6)
csv_data = response.text
df = pd.read_csv(StringIO(csv_data))
s3_client = boto3.client('s3')
s3_client.put_object(Bucket=s3_bucket_name, Key=s3_csv_key6, Body=csv_data)

s3_csv_key7 = "raw/Ventas_Provincias/PedidoDetalleProvincias.csv"
response = requests.get(git_csv_url7)
csv_data = response.text
df = pd.read_csv(StringIO(csv_data))
s3_client = boto3.client('s3')
s3_client.put_object(Bucket=s3_bucket_name, Key=s3_csv_key7, Body=csv_data)

job.commit()