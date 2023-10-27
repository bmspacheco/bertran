import sys
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
job.init(args['JOB_NAME'], args)

logger = glueContext.get_logger()

SOURCE_S3 = "data-lake33-utec"
SINK_S3 = "data-lake33-utec"
    
sourceS3Path = f's3://{SOURCE_S3}/raw/'

sinkS3Path = f's3://{SINK_S3}/stage/'

logger.info(f'sourceS3Path={sourceS3Path}' )
logger.info(f'sinkS3Path={sinkS3Path}' )

# Script generated for node CapaRaw_Locaciones
CapaRaw_Locaciones = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": [f"{sourceS3Path}/Tablas_Generales/Locaciones.csv"],
        "recurse": True,
    },
    transformation_ctx="CapaRaw_Locaciones",
)

# Script generated for node CapaRaw_Clientes
CapaRaw_Clientes = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": [f"{sourceS3Path}/Tablas_Generales/Clientes.csv"],
        "recurse": True,
    },
    transformation_ctx="CapaRaw_Clientes",
)

# Script generated for node CapaRaw_Productos
CapaRaw_Productos = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": [f"{sourceS3Path}/Tablas_Generales/Productos.csv"],
        "recurse": True,
    },
    transformation_ctx="CapaRaw_Productos",
)

# Script generated for node CapaRaw_PedidosCabecera
CapaRaw_PedidosCabeceraLima = (
    glueContext.create_dynamic_frame.from_options(
        format_options={
            "quoteChar": '"',
            "withHeader": True,
            "separator": ",",
            "optimizePerformance": False,
        },
        connection_type="s3",
        format="csv",
        connection_options={
            "paths": [f"{sourceS3Path}/Ventas_Lima/PedidoCabeceraLima.csv"],
            "recurse": True,
        },
        transformation_ctx="CapaRaw_PedidosCabecera_node1696309813205",
    )
)

CapaRaw_PedidosDetalleLima = (
    glueContext.create_dynamic_frame.from_options(
        format_options={
            "quoteChar": -1,
            "withHeader": True,
            "separator": ",",
            "optimizePerformance": False,
        },
        connection_type="s3",
        format="csv",
        connection_options={
            "paths": [f"{sourceS3Path}/Ventas_Lima/PedidoDetalleLima.csv"],
            "recurse": True,
        },
        transformation_ctx="CapaRaw_PedidosDetalleLima",
    )
)

# Script generated for node Rename Field
PedidosDetalleLima_RenameField1 = RenameField.apply(
    frame=CapaRaw_PedidosDetalleLima,
    old_name="Locacion",
    new_name="Descripcion2",
    transformation_ctx="PedidosDetalleLima_RenameField1",
)

# Script generated for node Rename Field
PedidosDetalleLima_RenameField2 = RenameField.apply(
    frame=PedidosDetalleLima_RenameField1,
    old_name="Cliente",
    new_name="NombreCliente2",
    transformation_ctx="PedidosDetalleLima_RenameField2",
)

# Script generated for node Rename Field
PedidosDetalleLima_RenameField3 = RenameField.apply(
    frame=PedidosDetalleLima_RenameField2,
    old_name="NombreProducto",
    new_name="NombreProducto2",
    transformation_ctx="PedidosDetalleLima_RenameField3",
)

# Script generated for node Rename Field
Clientes_RenameField1 = RenameField.apply(
    frame=CapaRaw_Clientes,
    old_name="Codigo",
    new_name="CodigoCliente",
    transformation_ctx="Clientes_RenameField1",
)

# Script generated for node Rename Field
Clientes_RenameField2 = RenameField.apply(
    frame=Clientes_RenameField1,
    old_name="Ruta",
    new_name="Ruta2",
    transformation_ctx="Clientes_RenameField2",
)

# Script generated for node Rename Field
Clientes_RenameField3 = RenameField.apply(
    frame=Clientes_RenameField2,
    old_name="Locacion",
    new_name="Locacion2",
    transformation_ctx="Clientes_RenameField3",
)

# Script generated for node Rename Field
PedidosCabeceraLima_RenameField1 = RenameField.apply(
    frame=CapaRaw_PedidosCabeceraLima,
    old_name="Locacion",
    new_name="Locacion3",
    transformation_ctx="PedidosCabeceraLima_RenameField1",
)


# Script generated for node Rename Field
PedidosCabeceraLima_RenameField2 = RenameField.apply(
    frame=PedidosCabeceraLima_RenameField1,
    old_name="NombreCliente",
    new_name="NombreCliente3",
    transformation_ctx="PedidosCabeceraLima_RenameField2",
)

Temp1 = CapaRaw_Locaciones.toDF()
Temp2 = Clientes_RenameField3.toDF()
Temp3 = CapaRaw_Productos.toDF()
Temp4 = CapaRaw_PedidosCabeceraLima.toDF()
Temp5 = PedidosDetalleLima_RenameField3.toDF()

Temp1.createOrReplaceTempView("Temp1")
Temp2.createOrReplaceTempView("Temp2")
Temp3.createOrReplaceTempView("Temp3")
Temp4.createOrReplaceTempView("Temp4")
Temp5.createOrReplaceTempView("Temp5")

Temp6 = spark.sql("select * from Temp5 T5, Temp1 T1 where T1.Descripcion == T5.Descripcion2")
Temp6.createOrReplaceTempView("Temp6")
Temp7 = spark.sql("select * from Temp6 T6, Temp2 T2 where T6.NombreCliente2 == T2.NombreCliente")
Temp7.createOrReplaceTempView("Temp7")
Temp8 = spark.sql("select * from Temp7 T7, Temp3 T3 where T7.NombreProducto2 == T3.NombreProducto")
Temp8.createOrReplaceTempView("Temp8")

Temp9 = PedidosCabeceraLima_RenameField2.toDF()

Temp9.createOrReplaceTempView("Temp9")
Temp10 = spark.sql("select * from Temp9 T9, Temp1 T1 where T9.Locacion3 == T1.Descripcion")
Temp10.createOrReplaceTempView("Temp10")
Temp11 = spark.sql("select * from Temp10 T10, Temp2 T2 where T10.Cliente == T2.NombreCliente")
Temp11.createOrReplaceTempView("Temp11")

PedidosCabeceraLima = spark.sql("SELECT Locacion, Fecha, NroPedido, Ruta, CodigoCliente, TotalUnidades FROM Temp11")
PedidosCabeceraLima = PedidosCabeceraLima.withColumn("temp_Fecha", F.to_date(PedidosCabeceraLima["Fecha"], "yyyyMMdd"))
PedidosCabeceraLima = PedidosCabeceraLima.withColumn("Fecha", F.to_date(PedidosCabeceraLima["temp_Fecha"], "yyyy-MM-dd"))
PedidosCabeceraLima = PedidosCabeceraLima.drop("temp_Fecha")
PedidosCabeceraLima = PedidosCabeceraLima.dropDuplicates()

PedidosDetalleLima = spark.sql("SELECT NroPedido, CodigoProducto, CanCaja, CantUnid, Fecha, Ruta, Locacion, CodigoCliente FROM Temp8")
PedidosDetalleLima = PedidosDetalleLima.withColumn("temp_Fecha", F.to_date(PedidosDetalleLima["Fecha"], "yyyyMMdd"))
PedidosDetalleLima = PedidosDetalleLima.withColumn("Fecha", F.to_date(PedidosDetalleLima["temp_Fecha"], "yyyy-MM-dd"))
PedidosDetalleLima = PedidosDetalleLima.drop("temp_Fecha")
PedidosDetalleLima = PedidosDetalleLima.dropDuplicates()

DynamicFrame_PedidosCabeceraLima = DynamicFrame.fromDF(PedidosCabeceraLima, glueContext, "DynamicFrame_PedidosCabeceraLima")
ChangeSchema1 = ApplyMapping.apply(
    frame=DynamicFrame_PedidosCabeceraLima,
    mappings=[
        ("Locacion", "string", "Locacion", "string"),
        ("Fecha", "date", "Fecha", "date"),
        ("NroPedido", "string", "NroPedido", "int"),    
        ("Ruta", "string", "Ruta", "int"),     
        ("CodigoCliente", "int", "CodigoCliente", "int"),
        ("TotalUnidades", "string", "TotalUnidades", "float"),
    ],
    transformation_ctx="ChangeSchema1",
)
DynamicFrame_PedidosDetalleLima = DynamicFrame.fromDF(PedidosDetalleLima, glueContext, "DynamicFrame_PedidosDetalleLima")
ChangeSchema2 = ApplyMapping.apply(
    frame=DynamicFrame_PedidosDetalleLima,
    mappings=[
        ("NroPedido", "string", "NroPedido", "int"),
        ("CodigoProducto", "string", "CodigoProducto", "int"),
        ("CanCaja", "string", "CanCaja", "int"),    
        ("CantUnid", "string", "CantUnid", "int"),     
        ("Fecha", "date", "Fecha", "date"),
        ("Ruta", "string", "Ruta", "int"),
        ("Locacion", "string", "Locacion", "string"),        
        ("CodigoCliente", "string", "CodigoCliente", "int"),
    ],
    transformation_ctx="ChangeSchema2",
)


#remueve archivos de ejecucion previa
glueContext.purge_s3_path(f"{sinkS3Path}/VentasLima/PedidosCabecera/",  {"retentionPeriod": 0, "excludeStorageClasses": ()})
glueContext.purge_s3_path(f"{sinkS3Path}/VentasLima/PedidosDetalle/",  {"retentionPeriod": 0, "excludeStorageClasses": ()})

# Script generated for node CapaSatge_PedidoDetalleLima
CapaStage_PedidoCabeceraLima_s3 = (
    glueContext.write_dynamic_frame.from_options(
        frame=ChangeSchema1,
        connection_type="s3",
        format="csv",
        connection_options={
            "path": f"{sinkS3Path}/VentasLima/PedidosCabecera/",
            "partitionKeys": [],
        },
        transformation_ctx="CapaStage_PedidoCabeceraLima_s3",
    )
)

# Script generated for node CapaSatge_PedidoDetalleLima
CapaStage_PedidoDetalleLima_s3 = (
    glueContext.write_dynamic_frame.from_options(
        frame=ChangeSchema2,
        connection_type="s3",
        format="csv",
        connection_options={
            "path": f"{sinkS3Path}/VentasLima/PedidosDetalle/",
            "partitionKeys": [],
        },
        transformation_ctx="CapaStage_PedidoDetalleLima_s3",
    )
)

glueContext.purge_s3_path(f"{sinkS3Path}/Tablas_Generales/Locaciones/",  {"retentionPeriod": 0, "excludeStorageClasses": ()})
glueContext.purge_s3_path(f"{sinkS3Path}/Tablas_Generales/Clientes/",  {"retentionPeriod": 0, "excludeStorageClasses": ()})
glueContext.purge_s3_path(f"{sinkS3Path}/Tablas_Generales/Productos/",  {"retentionPeriod": 0, "excludeStorageClasses": ()})

CapaStage_TablasGenerales_Locaciones_s3 = (
    glueContext.write_dynamic_frame.from_options(
        frame=CapaRaw_Locaciones,
        connection_type="s3",
        format="csv",
        connection_options={
            "path": f"{sinkS3Path}/Tablas_Generales/Locaciones/",
            "partitionKeys": [],
        },
        transformation_ctx="CapaStage_TablasGenerales_Locaciones_s3",
    )
)

CapaStage_TablasGenerales_Clientes_s3 = (
    glueContext.write_dynamic_frame.from_options(
        frame=Clientes_RenameField3,
        connection_type="s3",
        format="csv",
        connection_options={
            "path": f"{sinkS3Path}/Tablas_Generales/Clientes/",
            "partitionKeys": [],
        },
        transformation_ctx="CapaStage_TablasGenerales_Clientes_s3",
    )
)

CapaStage_TablasGenerales_Productos_s3 = (
    glueContext.write_dynamic_frame.from_options(
        frame=CapaRaw_Productos,
        connection_type="s3",
        format="csv",
        connection_options={
            "path": f"{sinkS3Path}/Tablas_Generales/Productos/",
            "partitionKeys": [],
        },
        transformation_ctx="CapaStage_TablasGenerales_Productos_s3",
    )
)

job.commit()