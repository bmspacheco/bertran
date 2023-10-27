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
    
if ('--DATA_ENV' in sys.argv):
    DATA_ENV = getResolvedOptions(sys.argv, ['DATA_ENV'])['DATA_ENV']

logger.info(f'Procesando para ambiente={DATA_ENV}')

sourceS3Path = f's3://{SOURCE_S3}/raw'
sourceS3Path2 = f's3://{SOURCE_S3}/stage'
sinkS3Path2 = f's3://{SINK_S3}/business'


logger.info(f'sourceS3Path={sourceS3Path}' )
logger.info(f'sourceS3Path2={sourceS3Path2}' )
logger.info(f'sinkS3Path={sinkS3Path2}' )


CapaRaw_PedidosDetalleProvincias = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": [f"{sourceS3Path}/Ventas_Provincias/PedidoDetalleProvincias.csv"],
        "recurse": True,
    },
    transformation_ctx="CapaRaw_PedidosDetalleProvincias",
)

CapaRaw_PedidosCabeceraProvincias = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": [f"{sourceS3Path}/Ventas_Provincias/PedidoCabeceraProvincias.csv"],
        "recurse": True,
    },
    transformation_ctx="CapaRaw_PedidosCabeceraProvincias",
)


CapaStage_PedidosDetalleLima = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": [f"{sourceS3Path2}/VentasLima/PedidosDetalle/"],
        "recurse": True,
    },
    transformation_ctx="CapaStage_PedidosDetalleLima",
)

CapaStage_PedidosCabeceraLima = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": [f"{sourceS3Path2}/VentasLima/PedidosCabecera/"],
        "recurse": True,
    },
    transformation_ctx="CapaStage_PedidosCabeceraLima",
)

# Script generated for node Rename Field
PedidosDetalleProvincias_RenameField1 = RenameField.apply(
    frame=CapaRaw_PedidosDetalleProvincias,
    old_name="OUTNUM",
    new_name="CodigoCliente",
    transformation_ctx="PedidosDetalleProvincias_RenameField1",
)

Temp12 = PedidosDetalleProvincias_RenameField1.toDF()
Temp12.createOrReplaceTempView("Temp12")

PedidosDetalleProvincias = spark.sql("SELECT NroPedido, CodigoProducto, CanCaja, CantUnid, Fecha, Ruta, Locacion, CodigoCliente FROM Temp12")
PedidosDetalleProvincias = PedidosDetalleProvincias.withColumn("temp_Fecha", F.to_date(PedidosDetalleProvincias["Fecha"], "yyyyMMdd"))
PedidosDetalleProvincias = PedidosDetalleProvincias.withColumn("Fecha", F.to_date(PedidosDetalleProvincias["temp_Fecha"], "yyyy-MM-dd"))
PedidosDetalleProvincias = PedidosDetalleProvincias.drop("temp_Fecha")
PedidosDetalleProvincias = PedidosDetalleProvincias.dropDuplicates()

DynamicFrame_PedidosDetalleProvincias = DynamicFrame.fromDF(PedidosDetalleProvincias, glueContext, "DynamicFrame_PedidosDetalleProvincias")
ChangeSchema3 = ApplyMapping.apply(
    frame=DynamicFrame_PedidosDetalleProvincias,
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
    transformation_ctx="ChangeSchema3",
)

DynamicFrame_PedidosDetalleLima = DynamicFrame.fromDF(CapaStage_PedidosDetalleLima.toDF(), glueContext, "DynamicFrame_PedidosDetalleLima")
ChangeSchema10 = ApplyMapping.apply(
    frame=DynamicFrame_PedidosDetalleLima,
    mappings=[
        ("NroPedido", "string", "NroPedido", "int"),
        ("CodigoProducto", "string", "CodigoProducto", "int"),
        ("CanCaja", "string", "CanCaja", "int"),    
        ("CantUnid", "string", "CantUnid", "int"),     
        ("Fecha", "string", "Fecha", "date"),
        ("Ruta", "string", "Ruta", "int"),
        ("Locacion", "string", "Locacion", "string"),        
        ("CodigoCliente", "string", "CodigoCliente", "int"),
    ],
    transformation_ctx="ChangeSchema10",
)


UnionDetalle = ChangeSchema10.toDF().union(ChangeSchema3.toDF())
PedidoDetalle = DynamicFrame.fromDF(UnionDetalle, glueContext, "PedidoDetalle")

Temp13 = CapaRaw_PedidosCabeceraProvincias.toDF()
Temp13.createOrReplaceTempView("Temp13")

PedidosCabeceraProvincias = spark.sql("SELECT Locacion, Fecha, NroPedido, Ruta, CodigoCliente, TotalUnidades  FROM Temp13")
PedidosCabeceraProvincias = PedidosCabeceraProvincias.withColumn("temp_Fecha", F.to_date(PedidosCabeceraProvincias["Fecha"], "yyyyMMdd"))
PedidosCabeceraProvincias = PedidosCabeceraProvincias.withColumn("Fecha", F.to_date(PedidosCabeceraProvincias["temp_Fecha"], "yyyy-MM-dd"))
PedidosCabeceraProvincias = PedidosCabeceraProvincias.drop("temp_Fecha")
PedidosCabeceraProvincias = PedidosCabeceraProvincias.dropDuplicates()

DynamicFrame_PedidosCabeceraProvincias = DynamicFrame.fromDF(PedidosCabeceraProvincias, glueContext, "DynamicFrame_PedidosCabeceraProvincias")
ChangeSchema5 = ApplyMapping.apply(
    frame=DynamicFrame_PedidosCabeceraProvincias,
    mappings=[
        ("Locacion", "string", "Locacion", "string"),
        ("Fecha", "date", "Fecha", "date"),
        ("NroPedido", "string", "NroPedido", "int"),    
        ("Ruta", "string", "Ruta", "int"),     
        ("CodigoCliente", "string", "CodigoCliente", "int"),
        ("TotalUnidades", "string", "TotalUnidades", "float"),
    ],
    transformation_ctx="ChangeSchema5",
)

DynamicFrame_PedidosCabeceraLima = DynamicFrame.fromDF(CapaStage_PedidosCabeceraLima.toDF(), glueContext, "DynamicFrame_PedidosCabeceraLima")
ChangeSchema11 = ApplyMapping.apply(
    frame=DynamicFrame_PedidosCabeceraLima,
    mappings=[
        ("Locacion", "string", "Locacion", "string"),
        ("Fecha", "string", "Fecha", "date"),
        ("NroPedido", "string", "NroPedido", "int"),    
        ("Ruta", "string", "Ruta", "int"),     
        ("CodigoCliente", "string", "CodigoCliente", "int"),
        ("TotalUnidades", "string", "TotalUnidades", "float"),
    ],
    transformation_ctx="ChangeSchema11",
)



UnionCabecera = ChangeSchema11.toDF().union(ChangeSchema5.toDF())
PedidoCabecera = DynamicFrame.fromDF(UnionCabecera, glueContext, "PedidoCabecera")



#limpieza de archivos previos
glueContext.purge_s3_path(f"{sinkS3Path2}/VentasTotales/PedidoCabecera_csv/",  {"retentionPeriod": 0, "excludeStorageClasses": ()})
glueContext.purge_s3_path(f"{sinkS3Path2}/VentasTotales/PedidoDetalle_csv/",  {"retentionPeriod": 0, "excludeStorageClasses": ()})


CapaBusiness_PedidoCabecera_s3 = (
    glueContext.write_dynamic_frame.from_options(
        frame=PedidoCabecera,
        connection_type="s3",
        format="csv",
        connection_options={
            "path": f"{sinkS3Path2}/VentasTotales/PedidoCabecera_csv/",
            "partitionKeys": [],
        },
        transformation_ctx="CapaBusiness_PedidoCabecera_s3_csv",
    )
)

# Script generated for node CapaSatge_PedidoDetalleLima
CapaBusiness_PedidoDetalle_s3 = (
    glueContext.write_dynamic_frame.from_options(
        frame=PedidoDetalle,
        connection_type="s3",
        format="csv",
        connection_options={
            "path": f"{sinkS3Path2}/VentasTotales/PedidoDetalle_csv/",
            "partitionKeys": [],
        },
        transformation_ctx="CapaBusiness_PedidoDetalle_s3_csv",
    )
)


#limpieza de archivos previos
glueContext.purge_s3_path(f"{sinkS3Path2}/VentasTotales/PedidoDetalle_parquet/",  {"retentionPeriod": 0, "excludeStorageClasses": ()})
glueContext.purge_s3_path(f"{sinkS3Path2}/VentasTotales/PedidoCabecera_parquet/",  {"retentionPeriod": 0, "excludeStorageClasses": ()})    


#escritura final de parquets

CapaBusiness_PedidoDetalle_parquet_s3 = (
    glueContext.write_dynamic_frame.from_options(
        frame=PedidoDetalle,
        connection_type="s3",
        format="parquet",
        connection_options={
            "path": f"{sinkS3Path2}/VentasTotales/PedidoDetalle_parquet/",
            "partitionKeys": [],
        },
        transformation_ctx="CapaBusiness_PedidoDetalle_parquet_s3",
    )
)

CapaBusiness_PedidoCabecera_parquet_s3 = (
    glueContext.write_dynamic_frame.from_options(
        frame=PedidoCabecera,
        connection_type="s3",
        format="parquet",
        connection_options={
            "path": f"{sinkS3Path2}/VentasTotales/PedidoCabecera_parquet/",
            "partitionKeys": [],
        },
        transformation_ctx="CapaBusiness_PedidoCabecera_parquet_s3",
    )
)

CapaStage_TablasGeneralesLocaciones = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": [f"{sourceS3Path2}/Tablas_Generales/Locaciones/"],
        "recurse": True,
    },
    transformation_ctx="CapaStage_TablasGeneralesLocaciones",
)

CapaStage_TablasGeneralesClientes = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": [f"{sourceS3Path2}/Tablas_Generales/Clientes/"],
        "recurse": True,
    },
    transformation_ctx="CapaStage_TablasGeneralesClientes",
)

CapaStage_TablasGeneralesProductos = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": [f"{sourceS3Path2}/Tablas_Generales/Productos/"],
        "recurse": True,
    },
    transformation_ctx="CapaStage_TablasGeneralesProductos",
)

ChangeSchema7 = ApplyMapping.apply(
    frame=CapaStage_TablasGeneralesLocaciones,
    mappings=[
        ("Locacion", "string", "Locacion", "string"),
        ("Descripcion", "string", "Descripcion", "string"),
    ],
    transformation_ctx="ChangeSchema7",
)

ChangeSchema8 = ApplyMapping.apply(
    frame=CapaStage_TablasGeneralesClientes,
    mappings=[
        ("Locacion2", "string", "Locacion", "string"),
        ("CodigoCliente", "string", "CodigoCliente", "int"),
        ("NombreCliente", "string", "NombreCliente", "string"),
        ("Ciudad", "string", "Ciudad", "string"),
        ("Ruta2", "string", "Ruta", "int"),
    ],
    transformation_ctx="ChangeSchema8",
)

ChangeSchema9 = ApplyMapping.apply(
    frame=CapaStage_TablasGeneralesProductos,
    mappings=[
        ("CodigoProducto", "string", "CodigoProducto", "int"),
        ("NombreProducto", "string", "NombreProducto", "string"),
        ("Presentacion", "string", "Presentacion", "int"),
    ],
    transformation_ctx="ChangeSchema9",
)

glueContext.purge_s3_path(f"{sinkS3Path2}/Tablas_Generales/Locaciones_csv/",  {"retentionPeriod": 0, "excludeStorageClasses": ()})
glueContext.purge_s3_path(f"{sinkS3Path2}/Tablas_Generales/Locaciones_parquet/",  {"retentionPeriod": 0, "excludeStorageClasses": ()})
glueContext.purge_s3_path(f"{sinkS3Path2}/Tablas_Generales/Clientes_csv/",  {"retentionPeriod": 0, "excludeStorageClasses": ()})
glueContext.purge_s3_path(f"{sinkS3Path2}/Tablas_Generales/Clientes_parquet/",  {"retentionPeriod": 0, "excludeStorageClasses": ()})
glueContext.purge_s3_path(f"{sinkS3Path2}/Tablas_Generales/Productos_csv/",  {"retentionPeriod": 0, "excludeStorageClasses": ()})
glueContext.purge_s3_path(f"{sinkS3Path2}/Tablas_Generales/Productos_parquet/",  {"retentionPeriod": 0, "excludeStorageClasses": ()})

CapaBusiness_TablasGenerales_Locaciones_csv_s3 = (
    glueContext.write_dynamic_frame.from_options(
        frame=ChangeSchema7,
        connection_type="s3",
        format="csv",
        connection_options={
            "path": f"{sinkS3Path2}/Tablas_Generales/Locaciones_csv/",
            "partitionKeys": [],
        },
        transformation_ctx="CapaBusiness_TablasGenerales_Locaciones_csv_s3",
    )
)

CapaBusiness_TablasGenerales_Clientes_csv_s3 = (
    glueContext.write_dynamic_frame.from_options(
        frame=ChangeSchema8,
        connection_type="s3",
        format="csv",
        connection_options={
            "path": f"{sinkS3Path2}/Tablas_Generales/Clientes_csv/",
            "partitionKeys": [],
        },
        transformation_ctx="CapaBusiness_TablasGenerales_Clientes_csv_s3",
    )
)

CapaBusiness_TablasGenerales_Productos_csv_s3 = (
    glueContext.write_dynamic_frame.from_options(
        frame=ChangeSchema9,
        connection_type="s3",
        format="csv",
        connection_options={
            "path": f"{sinkS3Path2}/Tablas_Generales/Productos_csv/",
            "partitionKeys": [],
        },
        transformation_ctx="CapaBusiness_TablasGenerales_Productos_csv_s3",
    )
)

CapaBusiness_TablasGenerales_Locaciones_parquet_s3 = (
    glueContext.write_dynamic_frame.from_options(
        frame=ChangeSchema7,
        connection_type="s3",
        format="parquet",
        connection_options={
            "path": f"{sinkS3Path2}/Tablas_Generales/Locaciones_parquet/",
            "partitionKeys": [],
        },
        transformation_ctx="CapaBusiness_TablasGenerales_Locaciones_parquet_s3",
    )
)

CapaBusiness_TablasGenerales_Clientes_parquet_s3 = (
    glueContext.write_dynamic_frame.from_options(
        frame=ChangeSchema8,
        connection_type="s3",
        format="parquet",
        connection_options={
            "path": f"{sinkS3Path2}/Tablas_Generales/Clientes_parquet/",
            "partitionKeys": [],
        },
        transformation_ctx="CapaBusiness_TablasGenerales_Clientes_parquet_s3",
    )
)

CapaBusiness_TablasGenerales_Productos_parquet_s3 = (
    glueContext.write_dynamic_frame.from_options(
        frame=ChangeSchema9,
        connection_type="s3",
        format="parquet",
        connection_options={
            "path": f"{sinkS3Path2}/Tablas_Generales/Productos_parquet/",
            "partitionKeys": [],
        },
        transformation_ctx="CapaBusiness_TablasGenerales_Productos_parquet_s3",
    )
)

job.commit()