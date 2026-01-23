from datetime import datetime
import shutil
import sys
import os
import csv
import tempfile
from pathlib import Path
import fastexcel 
import pandas as pd
from loguru import logger

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructType, StructField, DateType
from pyspark.sql import DataFrame
from delta.tables import DeltaTable

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

TEMP_DIR = Path("E:/Spark_Temp_Emision") 
TEMP_DIR.mkdir(parents=True, exist_ok=True)

HORA_INICIAL, HORA_FINAL = datetime.now(), datetime.now()
PERIODO = HORA_INICIAL.date()
PERIODO_STR = f"{HORA_INICIAL.year}{HORA_INICIAL.month:02d}{HORA_INICIAL.day:02d}"

BASE_PATH = Path(__file__).resolve().parent if "__file__" in locals() else Path.cwd()
PATH_SOURCE_CORE = BASE_PATH / 'Reportes_Core' 
PATH_SOURCE_SNTROS = BASE_PATH / 'Reportes_Siniestros' 
PATH_LAKE = BASE_PATH / 'DataLake_Local'
PATH_LOG = BASE_PATH / 'Logs' / f'LogAppPS_Emision_{PERIODO_STR}.log'

COLUMNS_CORE = ['ID','COD_RAMO','RAMO','PRODUCTO','POLIZA','CERTIFICADO','TIPO','CONTRATANTE','ASEGURADO',
                'DNI_ASEGURADO','RUC_ASEGURADO','F_EMISION_POLIZA','F_EMISION_CERT','F_INI_POLIZA',
                'F_FIN_POLIZA','F_ANULACION_CERT','F_INI_COBER','F_FIN_COBERT','PARENTESCO','FI']

COLUMNS_SNTROS = ['DNI','RAMO','POLIZA','CERTIFICADO','ASEGURADO','OCURRENCIA']

COLUMNS_FINAL = ['COD_RAMO','POLIZA','CERTIFICADO','ASEGURADO','TIPO_DOC','NRO_DOCUMENTO',
                'F_INI_COBER','F_FIN_COBERT','F_ANULACION_CERT','F_OCURRENCIA','PARENTESCO','FI','FECHA_REGISTRO','BASE']

COLUMNS_DATE_PARSE = ['F_ANULACION_CERT','F_INI_COBER','F_FIN_COBERT', 'F_EMISION_POLIZA', 'F_EMISION_CERT', 'F_INI_POLIZA', 'F_FIN_POLIZA']


def custom_format(type_process: int):
    def formatter(record: dict):
        levelname = record['level'].name
        if levelname == 'INFO':
            text = 'AVISO'
            level_str = f'<cyan>{text:<7}</cyan>'
            message_color = '<cyan>'
        elif levelname == 'WARNING':
            text = 'ALERTA'
            level_str = f'<level>{text:<7}</level>'
            message_color = '<level>'
        elif levelname == 'SUCCESS':
            text = 'ÉXITO'
            level_str = f'<level>{text:<7}</level>'
            message_color = '<level>'
        else:
            level_str = f'<level>{levelname:<7}</level>'
            message_color = '<level>'
        
        original_message = str(record['message'])
        safe_message = (original_message
                .replace("{", "{{")
                .replace("}", "}}")
                .replace("<", "\\<")
                .replace(">", "\\>")
               )
        custom_message = f"{message_color}{safe_message}</{message_color.strip('<>')}>\n"
        
        if type_process == 0:
            level_str = f'{level_str} | '
        else:
            level_str = f"{level_str} | {record['name']}:{record['function']}:{record['line']} - "
            if record["exception"] is not None:
                custom_message += f"{record['exception']}\n"

        return (
            f"<cyan><bold>{record['time']:DD/MM/YYYY HH:mm:ss}</bold></cyan> | "
            f"{level_str}"
            f"{custom_message}"
        )
    return formatter

def remove_log():
    logger.remove()

def add_log_console():
    logger.add(sys.stdout,
            backtrace=False, diagnose=False, level='DEBUG',
            colorize=True,
            format=custom_format(0))

def add_log_file(exits_log: bool):
    if PATH_LOG.exists() and not exits_log:
        logger.add(PATH_LOG, 
                backtrace=True, diagnose=True, level='INFO',
                format='\n\n{time:DD/MM/YYYY HH:mm:ss} | {level:<7} | {name}:{function}:{line} - {message}') 
        return
    
    logger.add(PATH_LOG, 
        backtrace=True, diagnose=True, level='INFO',
        format='{time:DD/MM/YYYY HH:mm:ss} | {level:<7} | {name}:{function}:{line} - {message}') 

def start_log(exits_log: bool = False):
    remove_log()
    add_log_console()
    add_log_file(exits_log)

start_log()
logger.info('Configurando entorno Spark local...')

spark = (
    SparkSession.builder 
    .appName("ETL_Medallion_Emision")
    .master("local[*]")
    .config("spark.driver.memory", "18g")
    .config("spark.local.dir", TEMP_DIR.as_posix())
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.shuffle.partitions", "10")
    .config("spark.sql.session.timeZone", "America/Lima")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.jars.packages", "io.delta:delta-spark_2.13:4.0.0")
    .getOrCreate()
)   

spark.sparkContext.setLogLevel("ERROR")
logger.info("Sesión Iniciada. ETL Lista.")

def detect_delimiter(archivo_path: Path, encoding: str = 'ISO-8859-1') -> str:
    try:
        with open(archivo_path, 'r', encoding=encoding) as f:
            linea_muestra = f.readline()
            dialecto = csv.Sniffer().sniff(linea_muestra, delimiters=',;\t|')
            return dialecto.delimiter
    except Exception:
        return ','

def clear_csv(path: Path, encoding_in: str = 'ISO-8859-1') -> bool:
    try:
        with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', delete=False, dir=path.parent) as temp_file:
            with open(path, 'r', encoding=encoding_in, errors='replace') as original:
                for line in original:
                    if line.startswith('"') and line.endswith('"\n'):
                        line = line[1:-2] + '\n'
                    elif line.startswith('"') and line.endswith('"'):
                        line = line[1:-1]
                    
                    line = line.replace('""', '"')
                    temp_file.write(line)
        
        shutil.move(temp_file.name, path)
        return True
    except Exception as e:
        raise Exception(f"Error limpiando archivo {path.name}: {e}")

def validate_path_delta(layer: str, table_name: str, show_message : bool = True) -> bool:
    path = f"{PATH_LAKE}/{layer}/{table_name}"
    is_delta = DeltaTable.isDeltaTable(spark, path)
    if show_message:
        if is_delta:
            logger.info(f"   ✅ La tabla Delta existe en {layer.upper()} / {table_name}.")
        else:
            logger.info(f"   ❌ La tabla Delta NO existe en {layer.upper()} / {table_name}.")
    return is_delta

def save_to_delta(df: DataFrame, layer: str, table_name: str, mode="overwrite"):
    logger.info(f"   ⏳ Preparando escritura en Delta ({table_name})...")
    path = f"{PATH_LAKE}/{layer}/{table_name}"
    df.persist()
    try:
        logger.info("   📋 Schema del DataFrame:")
        df.printSchema()

        total_rows = df.count()
        logger.info(f"   📊 Total Registros a procesar: {total_rows:,.0f}")
        logger.info("   ⏳ Escribiendo datos en Delta (Disco)...")
        df.write.format("delta").mode(mode).option("overwriteSchema", "true").save(path)
        logger.info(f"   💾 Guardado en {layer.upper()} / {table_name}")
    except Exception as e:
        raise Exception(f"Error guardando en Delta ({layer.upper()} / {table_name}): {e}")
    finally:
        df.unpersist()

def save_to_parquet(df: DataFrame, name_file: str, layer: str):
    try:
        logger.info("   ⏳ Preparando datos del origen...")
        df.persist()

        total_rows = df.count()
        logger.info(f"   📊 Total Registros a Exportar: {total_rows:,.0f}")

        logger.info(f"   ⏳ Generando reporte final Parquet: {name_file}...")
        final_file_path = BASE_PATH / "Consolidados_PySpark" / name_file
        final_file_path.parent.mkdir(parents=True, exist_ok=True)
        temp_output_folder = BASE_PATH  / "Consolidados_PySpark" / "Temp_Spark_Output"

        if layer == "Gold":
            logger.info("   ⏳ Ordenando datos (Sort)...")
            df = df.orderBy(
                F.asc_nulls_last("NRO_DOCUMENTO"), 
                F.asc("F_OCURRENCIA")
            )

        logger.info("   📦 Exportando archivo único Parquet...")
        (df.coalesce(1)
           .write
           .mode("overwrite")
           .option("compression", "zstd")
           .parquet(temp_output_folder.as_posix())
        )
        
        part_file = next(temp_output_folder.glob("part-*.parquet"), None)
        
        if part_file:
            if final_file_path.exists(): final_file_path.unlink()

            shutil.move(str(part_file), str(final_file_path))
            logger.debug(f"   Archivo Parquet generado correctamente: {final_file_path.name}")
            shutil.rmtree(temp_output_folder, ignore_errors=True)
        else:
            logger.warning("   ⚠️ No se generó el archivo Parquet correctamente.")
    except Exception as e:
        logger.error(f"   ❌ Error guardando Parquet ({final_file_path}): {e}")
    finally:
        df.unpersist()

def ingest_core_to_bronze() -> DataFrame:
    logger.info("   📖 Ingestando Core (CSVs)...")
    full_df: DataFrame
    
    try:
        if not PATH_SOURCE_CORE.exists():
            raise Exception(f"No existe carpeta: {PATH_SOURCE_CORE}")
        
        dfs = []
        schema_core = StructType([StructField(c, StringType(), True) for c in COLUMNS_CORE])

        for subfolder in PATH_SOURCE_CORE.iterdir():
            if subfolder.is_dir():
                csv_files = list(subfolder.glob("*.csv"))
                if not csv_files: continue
                
                logger.info(f"   📂 Leyendo: {subfolder.name} ({len(csv_files)} archivos)...")

                archivo_muestra = csv_files[0]
                delimiter = detect_delimiter(archivo_muestra)

                try:
                    # paths_str = [str(p) for p in csv_files
                    for archivo in csv_files:
                        clear_csv(archivo, encoding_in="ISO-8859-1")

                    df = (spark.read
                        .format("csv")
                        .option("header", "false")
                        # .option("recursiveFileLookup", "true") UTF-8
                        .option("delimiter", delimiter)
                        .option("encoding", "UTF-8")
                        .option("mode", "FAILFAST") 
                        .schema(schema_core)
                        .load(str(subfolder.as_posix()))
                        .withColumn("path_file", F.input_file_name())
                    )
                    dfs.append(df)
                except Exception as e:
                    raise Exception(f"Error leyendo subcarpeta {subfolder.name}: {e}")

        if not dfs: 
            raise Exception("No se encontraron archivos CSV válidos para cargar.")
        
        full_df: DataFrame = dfs[0]
        for d in dfs[1:]: 
            full_df = full_df.unionByName(d)

        full_df = (
            full_df
            .withColumn("FILE", F.element_at(F.split(F.col("path_file"), "/"), -1))
            .withColumn("FECHA_CARGA", F.from_utc_timestamp(F.current_timestamp(), 'America/Lima'))
            .drop("path_file")
        )

        save_to_delta(full_df, "Bronze", "Core_Raw")

        return full_df
    except Exception as e:
        raise Exception(f"Error en la Ingesta de Datos Core en la Capa Bronze: {e}") from e

def ingest_siniestros_to_bronze() -> DataFrame:
    logger.info("   📖 Ingestando Siniestros (Excel)...")
    
    try:
        if not PATH_SOURCE_SNTROS.exists(): 
            raise Exception(f"No existe carpeta: {PATH_SOURCE_SNTROS}")

        excels = [f for f in PATH_SOURCE_SNTROS.iterdir() if f.suffix in ['.xlsx','.xls']]
        
        dfs_pandas = []
        for excel in excels:
            logger.info(f"   📖 Leyendo: {excel.name}")
            reader = fastexcel.read_excel(excel)
            dtype_map = {c: "string" for c in COLUMNS_SNTROS}
            
            for sheet in reader.sheet_names:
                try:
                    data = reader.load_sheet_by_name(sheet, use_columns=COLUMNS_SNTROS, dtypes=dtype_map)
                    pdf: pd.DataFrame = data.to_pandas()

                    if pdf.empty: 
                        logger.warning(f"Hoja '{sheet}' está vacía, se omite.")
                        continue

                    pdf['FILE'] = excel.name
                    # pdf = pdf.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
                    
                    if not pdf.empty:
                        dfs_pandas.append(pdf)
                except Exception: 
                    logger.warning(f"No se pudo leer contenido de la hoja '{sheet}', se omite.\nArchivo Excel : ./{excel.parent.name}/{excel.name}")
                    continue

        if not dfs_pandas: 
            raise Exception("No se encontraron archivos Excel válidos para cargar")

        full_pdf: pd.DataFrame = pd.concat(dfs_pandas, ignore_index=True) #.astype(str)
        # full_pdf = full_pdf.replace({'nan': None, 'None': None, '': None})
        full_pdf = full_pdf.where(pd.notnull(full_pdf), None)

        schema_list = [StructField(c, StringType(), True) for c in COLUMNS_SNTROS]
        schema_list.append(StructField("FILE", StringType(), True))
        schema = StructType(schema_list)
        
        df_spark: DataFrame = spark.createDataFrame(full_pdf, schema=schema)

        logger.info("   🧹 Aplicando Trim (Strip)...")
        for col_name in df_spark.columns:
            df_spark = df_spark.withColumn(col_name, F.trim(F.col(col_name)))
                
        df_spark = df_spark.withColumn("FECHA_CARGA", F.from_utc_timestamp(F.current_timestamp(), 'America/Lima'))

        save_to_delta(df_spark, "Bronze", "Siniestros_Raw")

        return df_spark
    except Exception as e:
        raise Exception(f"Error en la Ingesta de Datos Siniestros en la Capa Bronze: {e}") from e

def transform_core_silver() -> DataFrame:
    logger.info("⚪ [SILVER] Transformando Core (Lógica Compleja)...")
    
    df = spark.read.format("delta").load(f"{PATH_LAKE}/Bronze/Core_Raw")
    
    # 1. Limpieza inicial (Strip) y Drop columnas basura
    for c in df.columns:
        df = df.withColumn(c, F.trim(F.col(c)))
    df = df.drop('ID','RAMO','PRODUCTO','TIPO')

    # -------------------------------------------------------------------------
    # RETO 1: DESPLAZAMIENTO DE COLUMNAS (MIGRACIÓN DE PL.STRUCT)
    # En Polars usabas struct().unnest(). En Spark calculamos las condiciones
    # booleanas y aplicamos F.when() columna por columna.
    # -------------------------------------------------------------------------
    
    # Expresiones Regex (Equivalentes a tus contains de Polars)
    regex_text = "^[a-zA-ZñÑáéíóúÁÉÍÓÚ\\s]+$"
    regex_digit = "^\\d+$"

    # Definimos aliases para facilitar lectura
    c_dni = F.col('DNI_ASEGURADO')
    c_ruc = F.col('RUC_ASEGURADO')

    # Condiciones Base
    is_text_dni = c_dni.rlike(regex_text)
    is_text_ruc = c_ruc.rlike(regex_text)
    is_digit_ruc = c_ruc.rlike(regex_digit)
    is_ce_ruc = c_ruc.contains("CE")
    is_ruc_null = c_ruc.isNull() | (c_ruc == "")

    # Condiciones de Desplazamiento (Idénticas a tu script V3)
    cond_shift_1 = is_text_dni & (is_digit_ruc | is_ruc_null | is_ce_ruc)
    cond_shift_2 = is_text_dni & is_text_ruc

    # Aplicamos el Shift columna por columna
    df = df.withColumns({
        # CONTRATANTE: Si shift_2 -> Unir Contratante + Asegurado
        "CONTRATANTE": F.when(cond_shift_2, F.concat_ws(" ", F.col("CONTRATANTE"), F.col("ASEGURADO")))
                        .otherwise(F.col("CONTRATANTE")),
        
        # ASEGURADO: Si shift_1 -> Asegurado + DNI. Si shift_2 -> DNI + RUC
        "ASEGURADO": F.when(cond_shift_1, F.concat_ws(" ", F.col("ASEGURADO"), c_dni))
                      .when(cond_shift_2, F.concat_ws(" ", c_dni, c_ruc))
                      .otherwise(F.col("ASEGURADO")),
        
        # DNI_ASEGURADO: Se mueve el RUC aquí en shift_1, o F_EMISION en shift_2
        "DNI_ASEGURADO": F.when(cond_shift_1, c_ruc)
                          .when(cond_shift_2, F.col("F_EMISION_POLIZA"))
                          .otherwise(c_dni),
                          
        # RUC_ASEGURADO
        "RUC_ASEGURADO": F.when(cond_shift_1, F.col("F_EMISION_POLIZA"))
                          .when(cond_shift_2, F.col("F_EMISION_CERT"))
                          .otherwise(c_ruc),
        
        # Rotación de fechas (Cascada)
        "F_EMISION_POLIZA": F.when(cond_shift_1, F.col("F_EMISION_CERT"))
                             .when(cond_shift_2, F.col("F_INI_POLIZA"))
                             .otherwise(F.col("F_EMISION_POLIZA")),
                             
        "F_EMISION_CERT": F.when(cond_shift_1, F.col("F_INI_POLIZA"))
                           .when(cond_shift_2, F.col("F_FIN_POLIZA"))
                           .otherwise(F.col("F_EMISION_CERT")),
                           
        "F_INI_POLIZA": F.when(cond_shift_1, F.col("F_FIN_POLIZA"))
                         .when(cond_shift_2, F.col("F_ANULACION_CERT"))
                         .otherwise(F.col("F_INI_POLIZA")),
                         
        "F_FIN_POLIZA": F.when(cond_shift_1, F.col("F_ANULACION_CERT"))
                         .when(cond_shift_2, F.col("F_INI_COBER"))
                         .otherwise(F.col("F_FIN_POLIZA")),
                         
        "F_ANULACION_CERT": F.when(cond_shift_1, F.col("F_INI_COBER"))
                             .when(cond_shift_2, F.col("F_FIN_COBERT"))
                             .otherwise(F.col("F_ANULACION_CERT")),
                             
        "F_INI_COBER": F.when(cond_shift_1, F.col("F_FIN_COBERT"))
                        .when(cond_shift_2, F.col("F_ANULACION_CERT")) # Nota: En tu V3 duplicabas lógica, he mantenido la estructura lógica
                        .otherwise(F.col("F_INI_COBER")),
                        
        "F_FIN_COBERT": F.when(cond_shift_1, F.col("PARENTESCO"))
                         .when(cond_shift_2, F.col("PARENTESCO"))
                         .otherwise(F.col("F_FIN_COBERT")),
                         
        "PARENTESCO": F.when(cond_shift_1 | cond_shift_2, F.col("FI"))
                       .otherwise(F.col("PARENTESCO")),
                       
        "FI": F.when(cond_shift_1 | cond_shift_2, F.lit(None))
               .otherwise(F.col("FI"))
    })

    # 2. Limpieza de DNI (Zfill)
    # Recargamos la columna c_dni porque acaba de cambiar en el paso anterior
    c_dni = F.col("DNI_ASEGURADO")
    df = df.withColumn("DNI_ASEGURADO", 
        F.when(
            (F.length(c_dni).isin([5, 6, 7])) & (c_dni.rlike(regex_digit)),
            F.lpad(c_dni, 8, '0')
        ).otherwise(c_dni)
    )

    # 3. Lógica Prioridad Documento (RUC vs DNI)
    # Definimos condiciones recalculadas post-shift
    c_dni = F.col('DNI_ASEGURADO')
    c_ruc = F.col('RUC_ASEGURADO')
    is_dni_digit = c_dni.rlike(regex_digit)
    is_ruc_digit = c_ruc.rlike(regex_digit)
    
    cond_prio_ruc = c_dni.isNull() & is_ruc_digit & (F.length(c_ruc) == 11)
    cond_prio_dni = c_ruc.isNull() & is_dni_digit & (F.length(c_dni) == 8)
    cond_cruce = is_dni_digit & is_ruc_digit

    # Calculamos NRO_DOCUMENTO y TIPO_DOC
    df = df.withColumn("NRO_DOCUMENTO",
        F.when(cond_prio_ruc, c_ruc)
         .when(cond_prio_dni, c_dni)
         .when(cond_cruce & (F.length(c_dni) == 11), c_ruc) # Prioridad RUC si ambos
         .when(cond_cruce & (F.length(c_dni) == 8), c_dni)
         .when(is_ruc_digit & c_dni.isNull(), c_ruc) # Default RUC
         .when(is_dni_digit & c_ruc.isNull(), c_dni) # Default DNI
         .otherwise(None)
    ).withColumn("TIPO_DOC",
        F.when(cond_prio_ruc, F.lit("RUC"))
         .when(cond_prio_dni, F.lit("DNI"))
         .when(cond_cruce, F.lit("RUC")) # Simplificado, ajusta según preferencia
         .when(F.col("NRO_DOCUMENTO") == c_ruc, F.lit("RUC"))
         .when(F.col("NRO_DOCUMENTO") == c_dni, F.lit("DNI"))
         .otherwise(None)
    )

    # Filtros de Calidad (V3 línea 376)
    df = df.filter(
        F.col("TIPO_DOC").isin(["DNI", "RUC"]) &
        (F.col("F_INI_COBER").isNotNull() | F.col("F_FIN_COBERT").isNotNull()) &
        (~F.col("NRO_DOCUMENTO").rlike(regex_text)) 
    )

    # 4. Parsing de Fechas (Formatos Múltiples)
    formats = ["yyyy-MM-dd", "dd/MM/yyyy", "dd-MM-yyyy", "yyyy/MM/dd", "dd/MM/yy"]
    
    def parse_date_col(c_name):
        # substring(1,10) igual que en tu V3
        return F.coalesce(*[F.to_date(F.substring(F.col(c_name),1,10), f) for f in formats])

    for c in COLUMNS_DATE_PARSE:
        df = df.withColumn(c, parse_date_col(c))

    # Limpieza Fechas Placeholder
    df = df.withColumn("F_FIN_COBERT", 
                       F.when(F.col("F_FIN_COBERT").isin(["2103-06-19", "2200-02-03", "4016-03-01"]), None)
                       .otherwise(F.col("F_FIN_COBERT")))
    
    # Validación Rangos Fechas (1900-9000)
    min_date, max_date = F.to_date(F.lit("1900-01-01")), F.to_date(F.lit("9000-12-31"))
    
    # Filtro estricto para INI_COBER
    df = df.filter(F.col("F_INI_COBER").between(min_date, max_date))

    # -------------------------------------------------------------------------
    # RETO 2: DATE RANGES Y EXPLODE (La parte crítica)
    # En Polars: pl.date_ranges(start, end, '1mo')
    # En Spark: sequence(start, end, interval '1 month')
    # -------------------------------------------------------------------------
    
    c_ini = F.col("F_INI_COBER")
    c_fin = F.col("F_FIN_COBERT")

    # Lógica de Inicio (Replica exacta V3): Si dia=1 o fin cercado -> ini. Sino -> 1ro del mes siguiente.
    cond_start = (
        (F.dayofmonth(c_ini) == 1) |
        ((c_fin == c_ini) & c_fin.isNotNull()) |
        (c_fin.isNotNull() & (F.datediff(c_fin, c_ini) <= 30) & (F.month(c_ini) >= F.month(c_fin)))
    )
    
    # F.trunc(add_months(x, 1), 'MM') nos da el 1er dia del mes siguiente.
    expr_start = F.when(cond_start, c_ini).otherwise(F.trunc(F.add_months(c_ini, 1), 'MM'))

    # Lógica de Fin (Replica exacta V3)
    cond_end = (c_fin.isNotNull() & (c_fin >= c_ini) & (c_fin <= F.add_months(c_ini, 85*12)))
    
    # Fallback si Fin es null o invalido: Misma lógica que inicio (para tener rango de 1 elemento)
    fallback_end = F.when(F.dayofmonth(c_ini) == 1, c_ini).otherwise(F.trunc(F.add_months(c_ini, 1), 'MM'))
    
    expr_end = F.when(cond_end, c_fin).otherwise(fallback_end)

    # Generación y Explosión
    # sequence() falla si start > end, así que aseguramos start <= end en la lógica
    # También validamos que temp_end >= temp_start por seguridad
    df = df.withColumn("TEMP_START", expr_start).withColumn("TEMP_END", expr_end)
    
    df = df.filter(F.col("TEMP_END") >= F.col("TEMP_START")) # Safety check

    # sequence genera array de timestamps/dates
    df = df.withColumn("F_OCURRENCIA_ARR", 
        F.sequence(F.col("TEMP_START"), F.col("TEMP_END"), F.expr("interval 1 month"))
    )
    
    df = df.withColumn("F_OCURRENCIA", F.explode("F_OCURRENCIA_ARR"))
    
    # Finalización
    df = df.withColumn("BASE", F.lit("CORE")) \
           .withColumn("FECHA_REGISTRO", F.current_date()) \
           .select([c for c in COLUMNS_FINAL if c in df.columns])

    save_to_delta(df, "Silver", "Core", mode="overwrite")
    return df

def transform_siniestros_silver() -> DataFrame:
    logger.info("⚪ [SILVER] Transformando Siniestros...")
    
    df = spark.read.format("delta").load(f"{PATH_LAKE}/Bronze/Siniestros_Raw")
    for c in df.columns: df = df.withColumn(c, F.trim(F.col(c)))

    # Parsing Fecha Ocurrencia -> F_INI_COBER
    formats = ["yyyy-MM-dd", "dd/MM/yyyy", "dd-MM-yyyy"]
    df = df.withColumn("F_INI_COBER", F.coalesce(*[F.to_date(F.substring(F.col("OCURRENCIA"),1,10), f) for f in formats]))

    # Validaciones básicas
    df = df.filter(F.col("F_INI_COBER").isNotNull() & (F.length(F.col("DNI")) >= 3))

    # Zfill DNI
    c_dni = F.col("DNI")
    df = df.withColumn("NRO_DOCUMENTO", 
        F.when((F.length(c_dni).isin([5,6,7])) & c_dni.rlike("^\\d+$"), F.lpad(c_dni, 8, '0'))
         .otherwise(c_dni)
    ).drop("DNI")

    # Columnas faltantes para consolidación
    df = df.withColumn("TIPO_DOC", F.lit("DNI")) \
           .withColumn("BASE", F.lit("SINIESTROS")) \
           .withColumn("F_FIN_COBERT", F.lit(None).cast(DateType())) \
           .withColumn("FECHA_REGISTRO", F.current_date())
           
    # Lógica de Expansión (Siniestros es puntual, rango de 1 mes efectivo)
    # Tu V3 usa la misma lógica que Core, pero como F_FIN_COBERT es Null,
    # el end_date hace fallback al start_logic.
    # Resultado: Start == End. Genera 1 registro.
    
    c_ini = F.col("F_INI_COBER")
    expr_start = F.when(F.dayofmonth(c_ini) == 1, c_ini).otherwise(F.trunc(F.add_months(c_ini, 1), 'MM'))
    
    df = df.withColumn("TEMP_START", expr_start) \
           .withColumn("TEMP_END", expr_start) # Mismo valor
    
    df = df.withColumn("F_OCURRENCIA", F.explode(
        F.sequence(F.col("TEMP_START"), F.col("TEMP_END"), F.expr("interval 1 month"))
    ))

    # Selección columnas existentes
    cols_sel = [c for c in COLUMNS_FINAL if c in df.columns]
    df = df.select(cols_sel).distinct() # Unique en V3

    save_to_delta(df, "Silver", "Siniestros", mode="overwrite")
    return df

def main():
    global HORA_FINAL

    RUN_BRONZE = True
    RUN_SILVER = False   
    RUN_GOLD = False

    RUN_CORE = True
    RUN_SINIESTROS = True

    ON_DEMAND = False

    try:
        if RUN_BRONZE:
            logger.info("🟠 Iniciando Capa Bronze (Incremental)...")
            
            if RUN_CORE:
                logger.info("   Procesando Core...")
                df_core_bronze = ingest_core_to_bronze()
            if RUN_SINIESTROS:
                logger.info("   Procesando Siniestros...")
                df_sint_bronze = ingest_siniestros_to_bronze()
        
        if RUN_SILVER:
            logger.info("⚪ Iniciando Capa Silver (Incremental)...")

            if df_core_bronze or validate_path_delta("Bronze", "Core_Raw"):
                transform_core_silver()
                
            if df_sint_bronze or validate_path_delta("Bronze", "Siniestros_Raw"):
                transform_siniestros_silver()
            
        if RUN_GOLD:
            logger.info("🟡 Iniciando Capa Gold...")
            
            # Reporte Core
            if validate_path_delta("Silver", "Core", False):
                df_core = spark.read.format("delta").load(f"{PATH_LAKE}/Silver/Core")
                save_to_parquet(df_core, f'Consolidado_Emision_Core_{PERIODO_STR}.parquet')
                
            # Reporte Siniestros
            if validate_path_delta("Silver", "Siniestros", False):
                df_sint = spark.read.format("delta").load(f"{PATH_LAKE}/Silver/Siniestros")
                save_to_parquet(df_sint, f'Consolidado_Emision_Siniestros_{PERIODO_STR}.parquet')
        
        logger.success('✅ Ejecución exitosa: Se procesó la información.')
        sys.exit(0)
    except Exception as e:
        logger.error(f"❌ Proceso Incompleto: {e}")
        sys.exit(1)
    finally:
        HORA_FINAL = datetime.now()
        difference_time = HORA_FINAL-HORA_INICIAL
        total_seconds = int(difference_time.total_seconds())
        difference_formated = "{} minuto(s), {} segundo(s)".format((total_seconds // 60), total_seconds % 60)
        logger.info(f"Tiempo de proceso: {difference_formated}")
        if TEMP_DIR.exists(): shutil.rmtree(TEMP_DIR, ignore_errors=True)

if __name__ == '__main__':
    main()