# from urllib.parse import quote_plus
from pathlib import Path
from loguru import logger
from rich import print
from rich.console import Console
from rich.panel import Panel
from rich.prompt import IntPrompt
from rich.text import Text
# from sqlalchemy import create_engine
# from sqlalchemy.sql import text
from contextlib import suppress
# from openpyxl import load_workbook
import polars as pl
import fastexcel
import datetime
# import chardet
from charset_normalizer import from_bytes
# from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
import os, sys, shutil, tempfile

# pyinstaller --noconfirm --onefile Carga_Reportes_Emision_BK.py --icon "Recursos/logo.ico"
# --hidden-import pyarrow.vendored.version

# uv run pyinstaller --noconfirm --onefile --strip --icon "Recursos/logo.ico" --hidden-import fastexcel Carga_Reportes_Emision_V3.py 
# uv run pyinstaller --noconfirm --onedir --noupx --strip --icon "Recursos/logo.ico" --hidden-import fastexcel Carga_Reportes_Emision_V3.py 
#--clean --log-level=DEBUG 

HORA_INICIAL, HORA_FINAL = datetime.datetime.now(), datetime.datetime.now()
PERIODO = str(HORA_INICIAL.year) + str(HORA_INICIAL.month).zfill(2) + str(HORA_INICIAL.day).zfill(2)
if getattr(sys, 'frozen', False): 
    PATH_GENERAL = Path(sys.executable).resolve().parent  #sys.argv[1]
else:
    PATH_GENERAL = Path(__file__).resolve().parent  #sys.argv[1]
PATH_SOURCE_CORE = PATH_GENERAL / 'Reportes_Core' 
PATH_SOURCE_SNTROS = PATH_GENERAL / 'Reportes_Siniestros' 
# PATH_DESTINATION =  Path(r'\\nt_nas\cobra001\Area-Cobranza-Automatizacion\MACRO_PA\EMISION\Consolidado de Polizas AP-Vida\Data') #sys.argv[2]
PATH_DESTINATION =  PATH_GENERAL / 'Consolidados'
# PATH_LOG = PATH_GENERAL / 'LogApp_{time:YYYYMMDD}.log'
PATH_LOG = PATH_GENERAL / 'Logs' / f'LogApp_{PERIODO}.log'
FILE_LOG_EXISTS = False
REPORT_NAME_CORE = f'Consolidado_Emision_Core_{PERIODO}.parquet' 
REPORT_NAME_SNTROS = f'Consolidado_Emision_Siniestros_{PERIODO}.parquet' 
TYPE_PROCESS_CSV = 0
FILES_TEMP_REMOVE = []
# PATH_REPORT_PREV = Path(tempfile.gettempdir()) / REPORT_NAME
# PATH_REPORT = PATH_DESTINATION / REPORT_NAME
# print(str(PATH_REPORT_PREV).replace('Emision','CORE'))
# print(str(PATH_REPORT_PREV).replace('Emision','SNTROS'))
# PATH_REPORT_BK = Path(tempfile.gettempdir()) / 'Consolidado_Final_Emision_BK.parquet'
# PATH_REPORT_PREV = PATH_DESTINATION / 'Consolidado_Final_Emision_BK.parquet'
# PATH_REPORT = PATH_DESTINATION / 'Consolidado_Final_Emision.parquet'
COLUMNS_CORE = ['ID','COD_RAMO','RAMO','PRODUCTO','POLIZA','CERTIFICADO','TIPO','CONTRATANTE','ASEGURADO',
                'DNI_ASEGURADO','RUC_ASEGURADO','F_EMISION_POLIZA','F_EMISION_CERT','F_INI_POLIZA',
                'F_FIN_POLIZA','F_ANULACION_CERT','F_INI_COBER','F_FIN_COBERT','PARENTESCO','FI']
COLUMNS_STRUCT = ['CONTRATANTE','ASEGURADO','DNI_ASEGURADO','RUC_ASEGURADO','F_EMISION_POLIZA',
                'F_EMISION_CERT','F_INI_POLIZA','F_FIN_POLIZA','F_ANULACION_CERT','F_INI_COBER','F_FIN_COBERT','PARENTESCO','FI']
COLUMNS_DATE = ['F_ANULACION_CERT','F_INI_COBER','F_FIN_COBERT']
COLUMNS_SNTROS = ['DNI','RAMO','POLIZA','CERTIFICADO','ASEGURADO','OCURRENCIA']
# COLUMNS_SNTROS = ['PERIODO','DOC - POLIZA','DNI','DNI CRUCE','DOC POLIZA CRUCE','ESTADO','RAMO','MONEDA',
#                   'Cod_Of','FILE SCTR','SINIESTRO','POLIZA','POLICY ID INSIS','CERTIFICADO','Poliza completa','ASEGURADO',
#                   'APERTURA','OCURRENCIA','RESERVA','COASEGURO','NETO.COASEG.','RETENCION','REASEGURO','EXCEDENTE','TOTAL REASEGURO',
#                   'CANAL COMERCIAL','DESCRP. CAN.COM','PRODUCTO','DESCRP. PRODUCTO','COD.SBS','FECHA DE DECLARACION','TIPO DOCUMENTO',
#                   'Fecha NAC ASEGURADO','CAUSA DE SINIESTRO','CONTRATANTE','EVENTOS EXTRAORDINARIOS','CONTIGENCIA','ACTIVIDAD SCTR',
#                   'TIPO DE SINIESTRO SCTR','FECHA CONVENIO MONEDA AJUSTADA / EMISION PÓLIZA','Moneda SCTR']
# COLUMNS_DATE = ['F_EMISION_POLIZA','F_EMISION_CERT','F_INI_POLIZA',
#            'F_FIN_POLIZA','F_ANULACION_CERT','F_INI_COBER','F_FIN_COBERT']
COLUMNS_FINAL = ['COD_RAMO','POLIZA','CERTIFICADO','ASEGURADO','TIPO_DOC','NRO_DOCUMENTO',
                'F_INI_COBER','F_FIN_COBERT','F_ANULACION_CERT','F_OCURRENCIA','PARENTESCO','FI','FECHA_REGISTRO','BASE']
FORMATS_DATE = ["%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%Y/%m/%d", 
                "%d/%m/%y", "%d-%m-%y", "%y/%m/%d", "%y-%m-%d"]
ROWS_LIMIT = 10_000_000
NUM_ROWS = 0
# FORMATS_REGEX_MAP = {
#     "%d/%m/%Y": r"^\d{2}/\d{2}/\d{4}$",
#     "%Y/%m/%d": r"^\d{4}/\d{2}/\d{2}$",
#     "%Y-%m-%d": r"^\d{4}-\d{2}-\d{2}$",
#     "%d-%m-%Y": r"^\d{2}-\d{2}-\d{4}$",
# }
# SCHEMA = {clave: pl.Utf8 for clave in COLUMNS_CORE}

def show_custom_rule(titulo, state = 'Success'):
    ancho_total = console.width
    if state == "Success":
        color_linea = "bold green"
        color_texto = "grey66"
    elif state == "Error":
        color_linea = "red"
        color_texto = "grey66"
    else:
        color_linea = "cyan"
        color_texto = "grey66"

    texto = f" {titulo} "
    largo_texto = len(Text.from_markup(texto).plain)

    largo_linea = max((ancho_total - largo_texto) // 2, 0)
    linea = "─" * largo_linea

    regla = f"[{color_linea}]{linea}[/{color_linea}][{color_texto}]{texto}[/{color_texto}][{color_linea}]{linea}[/{color_linea}]"
    console.print(regla)
    
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
        safe_message = original_message.replace("{", "{{").replace("}", "}}")
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
    global FILE_LOG_EXISTS
    if PATH_LOG.exists() and not exits_log:
        logger.add(PATH_LOG, 
                backtrace=True, diagnose=True, level='INFO',
                format='\n\n{time:DD/MM/YYYY HH:mm:ss} | {level:<7} | {name}:{function}:{line} - {message}') 
        return
    
    logger.add(PATH_LOG, 
        backtrace=True, diagnose=True, level='INFO',
        format='{time:DD/MM/YYYY HH:mm:ss} | {level:<7} | {name}:{function}:{line} - {message}') 
    FILE_LOG_EXISTS = True

def start_log(exits_log: bool = False):
    remove_log()
    add_log_console()
    add_log_file(exits_log)

# class ConnectionDB_SQLServer_SQLAlchemy:
#     def __init__(self):
#         self.driver=r'ODBC Driver 17 for SQL Server'
#         self.db_ip = '10.10.35.143'
#         self.db_name = 'FEBAN' #'BD_PROYINTERNO' 'BD_COBRANZA'
#         self.db_user = 'db_prd_adocast0'
#         self.db_password = '$L4p0s1tv420254#'
#         encoded_password = quote_plus(self.db_password)

#         self.string_connection = (
#             f'mssql+pyodbc://{self.db_user}:{encoded_password}@{self.db_ip}/{self.db_name}?driver={self.driver}'
#         )
#         #self.string_connection = f'mssql+pyodbc://{self.db_ip}/{self.db_name}?driver={self.driver}&Trusted_Connection=yes'
#         self.string_connection_pl = f'mssql://{self.db_ip}/{self.db_name}?trusted_connection=true'

#     def Open_Connection(self):
#         try:
#             self.engine = create_engine(self.string_connection)
#             self.conn = self.engine.connect()
#             stmt = text("SELECT 1")
#             self.conn.execute(stmt)
#         except Exception as e: 
#             raise Exception("Error al abrir conexión a la base de datos.\nError: "+str(e)) 

#     def Close_Connection(self):
#         try:
#             if self.conn:
#                 self.conn.close()
#             if self.engine:
#                 self.engine.dispose()
#         except Exception as e:
#             raise Exception("Error al cerrar la conexión.\nError: " + str(e))

#     def query_to_dataframe(self,query):
#         #Manejo de errores
#         self.estatus = None
#         try:
#             #self.Open_Connection()
#             # df = pd.read_sql_query(query, self.conn, chunksize=300000, dtype=str)
#             # db_df = pd.DataFrame()
#             # for chunk1 in df:
#             #     db_df = pd.concat([db_df, chunk1], ignore_index=True)
#             db_df = pl.read_database_uri(query,self.string_connection_pl, partition_on='CORTE',
#                                          partition_num=10, engine='connectorx')
            
#             # db_df = cx.read_sql(self.string_connection_pl, query, partition_on='CORTE',
#             #                              partition_num=10)
#             #db_df = pl.read_database(query,self.conn,batch_size=100000)

#             self.estatus = db_df
#         except Exception as e: 
#             # Manejar la excepción aquí
#             self.estatus = "Error al transformar query a dataframe.\nError: "+str(e)
#         finally:
#             # Cerramos la conexión
#             # self.conn.close()
#             # self.engine.dispose()

#             return self.estatus
        
# class ConnectionDB_SQLite_SQLAlchemy:
#     def __init__(self,key):
#         self.key = key

#     def Open_Connection(self):
#         try:
#             # self.conn = sqlite3.connect(PATH_BD)
#             # self.conn.execute(f"PRAGMA key='{self.key}'")

#             self.string_connection = f'sqlite:///{PATH_BD}'
#             self.engine = create_engine(self.string_connection) #isolation_level="AUTOCOMMIT"
#             self.conn = self.engine.connect()
#             # Session = sessionmaker(bind=self.engine)
#             # self.conn = Session()
#             self.conn.execute(text(f"PRAGMA key='{self.key}'"))
#             self.conn.execute(text('PRAGMA journal_mode = OFF;'))
#             self.conn.execute(text('PRAGMA synchronous = 0;'))
#             self.conn.execute(text('PRAGMA cache_size = 1000000;'))
#             self.conn.execute(text('PRAGMA locking_mode = EXCLUSIVE;'))
#             self.conn.execute(text('PRAGMA temp_store = MEMORY;'))
#         except Exception as e: 
#             raise Exception("Error al abrir conexión a la base de datos.\nError: "+str(e))
    
#     def execute_transaction(self,df):
#         #Manejo de errores
#         self.estatus = None
#         try:
#             self.Open_Connection()

#             # Crear un cursor para ejecutar comandos SQL
#             #self.cursor = self.conn.cursor()

#             # Verificamos si existe la tabla para vaciar su contenido
#             #self.cursor.execute("DROP TABLE IF EXISTS AFILIACIONES")

#             # Comenzar la transacción
#             #self.conn.execute("BEGIN")

#             self.conn.execute(text("DROP TABLE IF EXISTS AFILIACIONES"))

#             # df.to_sql('AFILIACIONES', self.conn, if_exists='replace', index=False)

#             # Insertamos el dataframe
#             mode = 'replace'
#             chunk_size = 150000 
#             for i in range(0, len(df), chunk_size):
#                 chunk = df[i:i + chunk_size]
#                 chunk.to_sql('AFILIACIONES', self.conn, if_exists=mode, index=False)
#                 mode = 'append'
#             # df.to_sql('AFILIACIONES', self.conn, if_exists='replace', chunksize=10000,
#             #           method='multi', index=False)

#             self.conn.commit()

#             # Confirmar la transacción
#             # self.conn.commit()

#             # Creamos los indexes
#             # self.cursor.execute("CREATE INDEX idx_docdeudor ON AFILIACIONES (DEUDOR_NIF)")
#             # self.cursor.execute("CREATE INDEX idx_codsociedad ON AFILIACIONES (DE_SOCIEDAD)")
#             # self.cursor.execute("CREATE INDEX idx_codpoliza ON AFILIACIONES (COD_POLIZA_CERT)")

#             self.conn.execute(text("CREATE INDEX idx_docdeudor ON AFILIACIONES (DEUDOR_NIF)"))
#             self.conn.execute(text("CREATE INDEX idx_codsociedad ON AFILIACIONES (DE_SOCIEDAD)"))
#             self.conn.execute(text("CREATE INDEX idx_codpoliza ON AFILIACIONES (COD_POLIZA_CERT)"))

#             self.conn.commit()
#             self.estatus = "Completado"
#         except Exception as e: 
#             # Manejar la excepción aquí
#             self.conn.rollback()
#             self.estatus = "Error al ejecutar transaccion.\nError: "+str(e)
#         finally:
#             # Cerramos el cursor
#             # self.cursor.close()
#             # Cerramos la conexión
#             self.conn.close()

#             #self.engine.dispose()

#             return self.estatus

class MenuPrompt(IntPrompt):
    validate_error_message = "[red]⛔ Error:[/red] Por favor ingrese un número válido."
    
    illegal_choice_message = (
        "[red]⛔ Error:[/red] Por favor seleccione una de las opciones disponibles."
    )

class Process_ETL:
    def __init__(self, process_type: str):
        try:
            self.process = int(process_type)
            if self.process not in [1,2,3]:
                raise Exception()
            self.Process_Start()
        except Exception:
            console.print()
            logger.error('Seleccione una opción válida.')
            print("[grey66]Presiona Enter para salir...[/grey66]")
            input()
            sys.exit(1)
    
    def Delete_Temp_Files(self, paths: list[Path]):
        for f in paths:
            f.unlink(missing_ok=True)
    
    def Detect_encoding(self, file_path: Path, read_size: int = 250_000) -> str:
        try:
            with open(file_path, 'rb') as f:
                raw = f.read(read_size)
            result = from_bytes(raw).best()
            encoding = result.encoding if result else None

            if encoding is None or encoding.lower() in ('johab', 'utf-16', 'utf-32', 'cp1250'):
                return 'ISO-8859-1'
            
            return encoding
        except Exception:
            return 'ISO-8859-1'
            
    def Read_CSV_Core(self, csv_path: Path, columns: list[str]) -> pl.LazyFrame:
        def is_csv_dirty(path: Path, encoding: str = 'utf-8') -> bool:
            try:
                with open(path, 'r', encoding=encoding, errors='replace') as f:
                    header = f.readline()

                    line = f.readline()
                    
                    if not line:
                        return False

                    clean_line = line.strip() 
                    
                    if clean_line.startswith('"') and clean_line.endswith('"'):
                        return True
                        
                    return False
            except Exception:
                return False

        def clear_csv(path: Path, encoding: str = 'utf-8') -> bool | None:
            try:
                with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', delete=False, dir=path.parent) as temp_file:
                    with open(path, 'r', encoding=encoding, errors='strict') as original:
                        for line in original:
                            if line.startswith('"') and line.endswith('"\n'):
                                line = line[1:-2] + '\n'
                            elif line.startswith('"') and line.endswith('"'):
                                line = line[1:-1]
                            line = line.replace('""', '"')
                            temp_file.write(line)

                # print(f"nuevo encode {self.Detect_encoding(temp_file.name)}")
                shutil.move(temp_file.name, path)

                # with open(path, encoding='utf-8') as f:
                #     for i, line in enumerate(f):
                #         if '' in line:
                #             print(f"Línea {i} contiene caracteres no convertibles")
                return True
            except Exception:
                return None

        def try_read_lazy(path: Path, encoding: str, is_dirty: bool) -> pl.LazyFrame | None:
            global TYPE_PROCESS_CSV
            try:
                if is_dirty:
                    raise Exception(f"Clean CSV")

                print(f"Process 1. {path.stem}")
                TYPE_PROCESS_CSV = 1
                encoding = 'utf8' if encoding.lower() == 'utf-8' else encoding
                lf = pl.scan_csv(path, infer_schema=False, truncate_ragged_lines=True)
                n_rows = lf.limit(1).collect(engine='streaming').height
                return lf if n_rows > 0 else None
            except Exception:
                status = clear_csv(path, encoding)
                if status is None:
                    raise Exception(f"Error al limpiar el archivo CSV. Codificación: {encoding}")
                try:
                    print(f"Process 2. {path.stem}")
                    TYPE_PROCESS_CSV = 2
                    return pl.scan_csv(path, infer_schema=False, truncate_ragged_lines=True) 
                except Exception:
                    print(f"Process 3. {path.stem}")
                    TYPE_PROCESS_CSV = 3
                    df = pl.read_csv(path, infer_schema=False, truncate_ragged_lines=True) #, encoding=encoding, low_memory=True
                    # print(df.estimated_size("mb"), "MB")
                    lf = df.lazy()
                    df.clear()
                    del df
                    # gc.collect()
                    return lf

        ct_encoding = self.Detect_encoding(csv_path)
        is_dirty = is_csv_dirty(csv_path, ct_encoding)
        # print(ct_encoding)

        try:
            lf = try_read_lazy(csv_path, ct_encoding, is_dirty)
            originales = lf.collect_schema().names()

            if len(originales) != len(columns):
                raise ValueError(f"Cantidad de columnas incorrecta. Permitido: {len(columns)}")

            mapping = dict(zip(originales, columns))
            lf: pl.LazyFrame = lf.rename(mapping)

            # print(lf.collect(engine='streaming').head())

            if TYPE_PROCESS_CSV > 1:
                n_rows = lf.select(columns).limit(1).collect(engine='streaming').height
            else:
                n_rows = 1

            return lf if n_rows > 0 else None
            # if n_rows > 0:
            #     return self.Transform_Dataframe_Core(lf)
            # else:
            #     return None
        except Exception as e:
            raise Exception(f"{e}\nArchivo CSV: ./{csv_path.parent.name}/{csv_path.name}") from e

    def Transform_Dataframe_Core(self, lf: pl.LazyFrame, subfolder_path: Path) -> pl.LazyFrame:
        global NUM_ROWS

        q = lf.drop(['ID','RAMO','PRODUCTO','TIPO'])
        q = (
            q
            .with_columns([
                pl.col(col).str.strip_chars() for col in q.collect_schema().names()
            ])
        )

        col_dni = pl.col('DNI_ASEGURADO')
        col_ruc = pl.col('RUC_ASEGURADO')

        expr_is_text_dni = col_dni.str.contains(r"^[a-zA-ZñÑáéíóúÁÉÍÓÚ\s]+$", literal=False)
        expr_is_text_ruc = col_ruc.str.contains(r"^[a-zA-ZñÑáéíóúÁÉÍÓÚ\s]+$", literal=False)

        expr_is_digit_ruc = col_ruc.cast(pl.Int64, strict=False).is_not_null()
        expr_is_ce_ruc = col_ruc.str.contains("CE", literal=True, strict=False)

        cond_shift_1 = expr_is_text_dni & (expr_is_digit_ruc | col_ruc.is_null() | expr_is_ce_ruc)
        cond_shift_2 = expr_is_text_dni & expr_is_text_ruc

        q = (
            q
            .with_columns(
                pl.when(cond_shift_1)
                .then(
                    pl.struct([
                        pl.col('CONTRATANTE'),
                        pl.concat_str([pl.col('ASEGURADO'), pl.lit(' '), col_dni]).alias('ASEGURADO'),
                        col_ruc.alias('DNI_ASEGURADO'),
                        pl.col('F_EMISION_POLIZA').alias('RUC_ASEGURADO'),
                        pl.col('F_EMISION_CERT').alias('F_EMISION_POLIZA'),
                        pl.col('F_INI_POLIZA').alias('F_EMISION_CERT'),
                        pl.col('F_FIN_POLIZA').alias('F_INI_POLIZA'),
                        pl.col('F_ANULACION_CERT').alias('F_FIN_POLIZA'),
                        pl.col('F_INI_COBER').alias('F_ANULACION_CERT'),
                        pl.col('F_FIN_COBERT').alias('F_INI_COBER'),
                        pl.col('PARENTESCO').alias('F_FIN_COBERT'),
                        pl.col('FI').alias('PARENTESCO'),
                        pl.lit(None).alias('FI'),
                    ])
                )
                .when(cond_shift_2)
                .then(
                    pl.struct([
                        pl.concat_str([pl.col('CONTRATANTE'), pl.lit(' '), pl.col('ASEGURADO')]).alias('CONTRATANTE'),
                        pl.concat_str([col_dni, pl.lit(' '), col_ruc]).alias('ASEGURADO'),
                        pl.col('F_EMISION_POLIZA').alias('DNI_ASEGURADO'),
                        pl.col('F_EMISION_CERT').alias('RUC_ASEGURADO'),
                        pl.col('F_INI_POLIZA').alias('F_EMISION_POLIZA'),
                        pl.col('F_FIN_POLIZA').alias('F_EMISION_CERT'),
                        pl.col('F_ANULACION_CERT').alias('F_INI_POLIZA'),
                        pl.col('F_INI_COBER').alias('F_FIN_POLIZA'),
                        pl.col('F_FIN_COBERT').alias('F_ANULACION_CERT'),
                        pl.col('F_ANULACION_CERT').alias('F_INI_COBER'),
                        pl.col('PARENTESCO').alias('F_FIN_COBERT'),
                        pl.col('FI').alias('PARENTESCO'),
                        pl.lit(None).alias('FI'),
                    ])
                )
                .otherwise(
                    pl.struct(COLUMNS_STRUCT)
                )
                .alias('custom_struct')
            )
            .drop(COLUMNS_STRUCT)
            .unnest('custom_struct')
            .drop(['CONTRATANTE', 'F_EMISION_POLIZA', 'F_EMISION_CERT', 'F_INI_POLIZA', 'F_FIN_POLIZA'], strict=False)
            .with_columns(
                pl.when(
                    pl.col('DNI_ASEGURADO').str.len_chars().is_in([5, 6, 7]) &
                    pl.col('DNI_ASEGURADO').cast(pl.Int64, strict=False).is_not_null()
                )
                .then(pl.col('DNI_ASEGURADO').str.zfill(8))
                .otherwise(pl.col('DNI_ASEGURADO'))
                .alias('DNI_ASEGURADO')
            )
            .with_columns([
                pl.when(pl.col(c) == '').then(None).otherwise(pl.col(c)).alias(c)
                for c in ['DNI_ASEGURADO', 'RUC_ASEGURADO']
            ])
        )

        # q = (
        #     q
        #     # .with_columns(pl.col('ID').cast(pl.Int32))
        #     # .with_columns(pl.col('RAMO').str.to_uppercase())
        #     # .with_columns(pl.col('PRODUCTO').str.to_uppercase())
        #     # .with_columns(pl.col('TIPO').str.to_uppercase())
        #     .with_columns(pl.when((pl.col('DNI_ASEGURADO').str.contains(r"^[a-zA-ZñÑáéíóúÁÉÍÓÚ\s]+$", literal=False)) &
        #                     (
        #                         (pl.col('RUC_ASEGURADO').str.contains(r"^\d+$", literal=False)) |
        #                         (pl.col('RUC_ASEGURADO').is_null()) |
        #                         (pl.col('RUC_ASEGURADO').str.contains("CE", literal=True, strict=False))
        #                     )
        #                 )
        #                 .then(pl.struct([
        #                     pl.col('CONTRATANTE'),
        #                     pl.concat_str([
        #                         pl.col('ASEGURADO'),
        #                         pl.lit(' '), 
        #                         pl.col('DNI_ASEGURADO')
        #                     ]).alias('ASEGURADO'), 
        #                     pl.col('RUC_ASEGURADO').alias('DNI_ASEGURADO'),
        #                     pl.col('F_EMISION_POLIZA').alias('RUC_ASEGURADO'),
        #                     pl.col('F_EMISION_CERT').alias('F_EMISION_POLIZA'),
        #                     pl.col('F_INI_POLIZA').alias('F_EMISION_CERT'),
        #                     pl.col('F_FIN_POLIZA').alias('F_INI_POLIZA'),
        #                     pl.col('F_ANULACION_CERT').alias('F_FIN_POLIZA'),
        #                     pl.col('F_INI_COBER').alias('F_ANULACION_CERT'),
        #                     pl.col('F_FIN_COBERT').alias('F_INI_COBER'),
        #                     pl.lit(None).alias('F_FIN_COBERT'),
        #                 ]))
        #                 .when((pl.col('DNI_ASEGURADO').str.contains(r"^[a-zA-ZñÑáéíóúÁÉÍÓÚ\s]+$", literal=False)) &
        #                       (pl.col('RUC_ASEGURADO').str.contains(r"^[a-zA-ZñÑáéíóúÁÉÍÓÚ\s]+$", literal=False)))
        #                 .then(pl.struct([
        #                     pl.concat_str([
        #                         pl.col('CONTRATANTE'),
        #                         pl.lit(' '), 
        #                         pl.col('ASEGURADO')
        #                     ]).alias('CONTRATANTE'), 
        #                     pl.concat_str([
        #                         pl.col('DNI_ASEGURADO'),
        #                         pl.lit(' '), 
        #                         pl.col('RUC_ASEGURADO')
        #                     ]).alias('ASEGURADO'), 
        #                     pl.col('F_EMISION_POLIZA').alias('DNI_ASEGURADO'),
        #                     pl.col('F_EMISION_CERT').alias('RUC_ASEGURADO'),
        #                     pl.col('F_INI_POLIZA').alias('F_EMISION_POLIZA'),
        #                     pl.col('F_FIN_POLIZA').alias('F_EMISION_CERT'),
        #                     pl.col('F_ANULACION_CERT').alias('F_INI_POLIZA'),
        #                     pl.col('F_INI_COBER').alias('F_FIN_POLIZA'),
        #                     pl.col('F_FIN_COBERT').alias('F_ANULACION_CERT'),
        #                     pl.col('F_ANULACION_CERT').alias('F_INI_COBER'),
        #                     pl.lit(None).alias('F_FIN_COBERT'),
        #                 ]))
        #                 .otherwise(pl.struct(COLUMNS_STRUCT)).alias('custom_struct'))
        #     .drop(COLUMNS_STRUCT)
        #     .unnest('custom_struct')
        #     .drop(['CONTRATANTE','F_EMISION_POLIZA','F_EMISION_CERT','F_INI_POLIZA','F_FIN_POLIZA'])
        #     .with_columns(
        #         pl.when(
        #             (pl.col('DNI_ASEGURADO').str.len_chars().is_in([5, 6, 7])) &
        #             (pl.col('DNI_ASEGURADO').str.contains(r"^\d+$", literal=False))
        #         )
        #         .then(pl.col('DNI_ASEGURADO').str.zfill(8))
        #         .otherwise(pl.col('DNI_ASEGURADO'))
        #         .alias('DNI_ASEGURADO')
        #     )
        #     .with_columns([
        #         pl.when(pl.col(c) == '').then(None).otherwise(pl.col(c)).alias(c)
        #         for c in ['DNI_ASEGURADO', 'RUC_ASEGURADO']
        #     ])
        # )

        # q = (
        #     q
        #     # .rename({'policy': 'POLIZA', 'certif': 'CERTIFICADO'})
        #     .with_columns(
        #         pl.when(
        #             (pl.col('DNI_ASEGURADO').str.len_chars().is_in([5, 6, 7])) &
        #             (pl.col('DNI_ASEGURADO').str.contains(r"^\d+$", literal=False))
        #         )
        #         .then(pl.col('DNI_ASEGURADO').str.zfill(8))
        #         .otherwise(pl.col('DNI_ASEGURADO'))
        #         .alias('DNI_ASEGURADO')
        #     )
        #     # .with_columns(pl.col('DNI_ASEGURADO').is_null().alias('ES_NULO_DNI'))
        #     # .with_columns(pl.col('RUC_ASEGURADO').is_null().alias('ES_NULO_RUC'))
        #     # .with_columns(pl.col('DNI_ASEGURADO').str.contains(r"^\d+$", literal=False).alias('ES_NUMERO_DNI'))
        #     # .with_columns(pl.col('RUC_ASEGURADO').str.contains(r"^\d+$", literal=False).alias('ES_NUMERO_RUC'))    
        #     # .with_columns(pl.col('F_EMISION_POLIZA').str.strptime(pl.Date, format='%d/%m/%Y', strict=False).cast(pl.Date))
        #     # .with_columns(pl.col('F_EMISION_CERT').str.strptime(pl.Date, format='%d/%m/%Y', strict=False).cast(pl.Date))
        #     # .with_columns(pl.col('F_INI_POLIZA').str.strptime(pl.Date, format='%d/%m/%Y', strict=False).cast(pl.Date))
        #     # .with_columns(pl.col('F_FIN_POLIZA').str.strptime(pl.Date, format='%d/%m/%Y', strict=False).cast(pl.Date))
        #     # .with_columns(pl.col('F_ANULACION_CERT').str.strptime(pl.Date, format='%d/%m/%Y').cast(pl.Date))
        #     # .with_columns(pl.col('F_INI_COBER').str.strptime(pl.Date, format='%d/%m/%Y').cast(pl.Date)) 
        #     # .with_columns(pl.col('F_FIN_COBERT').str.strptime(pl.Date, format='%d/%m/%Y').cast(pl.Date)) 
        # )
        
        # dni_vacio = (
        #     ((pl.col('DNI_ASEGURADO').is_null()) | (pl.col('DNI_ASEGURADO') == '')) &
        #     (pl.col('RUC_ASEGURADO').str.contains(r"^\d+$", literal=False)) & 
        #     (pl.col('RUC_ASEGURADO').str.len_chars() == 11)
        # )
        # ruc_vacio = (
        #     ((pl.col('RUC_ASEGURADO').is_null()) | (pl.col('RUC_ASEGURADO') == '')) & 
        #     (pl.col('DNI_ASEGURADO').str.contains(r"^\d+$", literal=False)) & 
        #     (pl.col('DNI_ASEGURADO').str.len_chars() == 8)
        # )
        # dni_valid = (
        #     (pl.col('DNI_ASEGURADO').str.contains(r"^\d+$", literal=False)) & 
        #     (pl.col('RUC_ASEGURADO').str.contains(r"^\d+$", literal=False)) &
        #     (pl.col('DNI_ASEGURADO').str.len_chars() == 8)
        # )
        # ruc_valid = (
        #     (pl.col('DNI_ASEGURADO').str.contains(r"^\d+$", literal=False)) & 
        #     (pl.col('RUC_ASEGURADO').str.contains(r"^\d+$", literal=False)) &
        #     (pl.col('DNI_ASEGURADO').str.len_chars() == 11)
        # )
        # dni_default = (
        #         (pl.col('DNI_ASEGURADO').str.contains(r"^\d+$", literal=False)) &
        #         (pl.col('RUC_ASEGURADO').str.contains(r"^\d+$", literal=False)).is_null()
        # )
        # ruc_default = (
        #         (pl.col('RUC_ASEGURADO').str.contains(r"^\d+$", literal=False)) &
        #         (pl.col('DNI_ASEGURADO').str.contains(r"^\d+$", literal=False)).is_null()
        # )
        # pass_dni = (
        #     (pl.col('DNI_ASEGURADO').str.contains("PAS", literal=True, strict=False)) &
        #     (pl.col('RUC_ASEGURADO').str.contains("PAS", literal=True, strict=False)).is_null()
        # )
        # pass_ruc = (
        #     (pl.col('RUC_ASEGURADO').str.contains("PAS", literal=True, strict=False)) &
        #     (pl.col('DNI_ASEGURADO').str.contains("PAS", literal=True, strict=False)).is_null()
        # )
        # ce_dni = (
        #     (
        #         (pl.col('DNI_ASEGURADO').str.contains("CE", literal=True, strict=False)) |
        #         (pl.col('DNI_ASEGURADO').str.contains("C.E", literal=True, strict=False))
        #         # (pl.col('DNI_ASEGURADO').str.contains_any(["C","E"], ascii_case_insensitive=True))
        #     ) &
        #     (pl.col('RUC_ASEGURADO').str.contains("CE", literal=True, strict=False)).is_null()
        # )
        # ce_ruc = (
        #     (
        #         (pl.col('RUC_ASEGURADO').str.contains("CE", literal=True, strict=False)) |
        #         (pl.col('RUC_ASEGURADO').str.contains("C.E", literal=True, strict=False))
        #         # (pl.col('RUC_ASEGURADO').str.contains_any(["C","E"], ascii_case_insensitive=True))
        #     ) &
        #     (pl.col('DNI_ASEGURADO').str.contains("CE", literal=True, strict=False)).is_null()
        # )

        # q = (
        #     q
        #     .with_columns([
        #         pl.when(dni_vacio | ruc_valid)
        #         .then(pl.col('RUC_ASEGURADO'))
        #         .when(ruc_vacio | dni_valid)
        #         .then(pl.col('DNI_ASEGURADO'))
        #         .when(dni_default)
        #         .then(pl.col('DNI_ASEGURADO'))
        #         .when(ruc_default)
        #         .then(pl.col('RUC_ASEGURADO'))
        #         .when(pass_dni)
        #         .then(pl.col('DNI_ASEGURADO'))
        #         .when(pass_ruc)
        #         .then(pl.col('RUC_ASEGURADO'))
        #         .when(ce_dni)
        #         .then(pl.col('DNI_ASEGURADO'))
        #         .when(ce_ruc)
        #         .then(pl.col('RUC_ASEGURADO'))
        #         .otherwise(pl.lit(None)).alias('NRO_DOCUMENTO'),
        #         pl.when(dni_vacio | ruc_valid)
        #         .then(pl.lit('RUC'))
        #         .when(ruc_vacio | dni_valid)
        #         .then(pl.lit('DNI'))
        #         .when(pass_dni | pass_ruc)
        #         .then(pl.lit('DNI'))
        #         .when(ce_dni | ce_ruc)
        #         .then(pl.lit('DNI'))
        #         .otherwise(pl.lit(None)).alias('TIPO_DOC')
        #     ])
        #     .drop(['DNI_ASEGURADO','RUC_ASEGURADO']) 
        #     # .with_columns(pl.col('TIPO_DOC').cast(pl.Categorical))
        #     .filter(
        #         # (pl.col('TIPO_DOC').is_in(['DNI','RUC']))
        #         ((pl.col('TIPO_DOC') == 'DNI') | (pl.col('TIPO_DOC') == 'RUC'))
        #         &
        #         ~((pl.col('F_INI_COBER').is_null()) & (pl.col('F_FIN_COBERT').is_null()))
        #         &
        #         ~(pl.col('NRO_DOCUMENTO').str.contains(r"^[a-zA-ZñÑáéíóúÁÉÍÓÚ\s]+$", literal=False))
        #     )
        #     # .filter( ~((pl.col('F_INI_COBER').is_null()) & (pl.col('F_FIN_COBERT').is_null())) )
        #     # .filter( ~(pl.col('NRO_DOCUMENTO').str.contains(r"^[a-zA-ZñÑáéíóúÁÉÍÓÚ\s]+$", literal=False)) )
        # )

        col_dni = pl.col('DNI_ASEGURADO')
        col_ruc = pl.col('RUC_ASEGURADO')

        is_dni_null = col_dni.is_null()
        is_ruc_null = col_ruc.is_null()

        is_dni_digit = col_dni.str.contains(r"^\d+$")
        is_ruc_digit = col_ruc.str.contains(r"^\d+$")

        len_dni = col_dni.str.len_chars()
        len_ruc = col_ruc.str.len_chars()

        is_dni_pas = col_dni.str.contains("PAS", literal=True)
        is_ruc_pas = col_ruc.str.contains("PAS", literal=True)
        is_dni_ce  = col_dni.str.contains("CE", literal=True) | col_dni.str.contains("C.E", literal=True)
        is_ruc_ce  = col_ruc.str.contains("CE", literal=True) | col_ruc.str.contains("C.E", literal=True)

        cond_prioridad_ruc = is_dni_null & is_ruc_digit & (len_ruc == 11)
        cond_prioridad_dni = is_ruc_null & is_dni_digit & (len_dni == 8)

        cond_cruce_ruc = is_dni_digit & is_ruc_digit & (len_dni == 11)
        cond_cruce_dni = is_dni_digit & is_ruc_digit & (len_dni == 8)

        cond_default_ruc = is_ruc_digit & is_dni_null
        cond_default_dni = is_dni_digit & is_ruc_null

        cond_special_ruc = (is_ruc_pas | is_ruc_ce) & is_dni_null
        cond_special_dni = (is_dni_pas | is_dni_ce) & is_ruc_null

        q = (
            q
            .with_columns([
                pl.when(pl.col(c) == '').then(None).otherwise(pl.col(c)).alias(c)
                for c in ['DNI_ASEGURADO', 'RUC_ASEGURADO']
            ])
            .with_columns([
                pl.when(cond_prioridad_ruc | cond_cruce_ruc | cond_default_ruc | cond_special_ruc)
                .then(col_ruc)
                .when(cond_prioridad_dni | cond_cruce_dni | cond_default_dni | cond_special_dni)
                .then(col_dni)
                .otherwise(None)
                .alias('NRO_DOCUMENTO'),
                
                pl.when(cond_prioridad_ruc | cond_cruce_ruc) 
                .then(pl.lit('RUC'))
                .when(cond_prioridad_dni | cond_cruce_dni) 
                .then(pl.lit('DNI'))
                .when(cond_special_dni | cond_special_ruc)
                .then(pl.lit('DNI'))
                .when(cond_default_ruc).then(pl.lit('RUC'))
                .when(cond_default_dni).then(pl.lit('DNI'))
                .otherwise(None)
                .alias('TIPO_DOC')
            ])
            .drop(['DNI_ASEGURADO', 'RUC_ASEGURADO'])
            .filter(
                pl.col('TIPO_DOC').is_in(['DNI', 'RUC']) ,
                (pl.col('F_INI_COBER').is_not_null() | pl.col('F_FIN_COBERT').is_not_null()) ,
                ~pl.col('NRO_DOCUMENTO').str.contains(r"^[a-zA-ZñÑáéíóúÁÉÍÓÚ\s]+$")
            )
        )

        NUM_ROWS = int(q.select(pl.len()).collect(engine='streaming').item())
        print(f"Número de filas: {NUM_ROWS}")
        logger.info(f"Transformando datos de subcarpeta '{subfolder_path.name}'...")
        if NUM_ROWS < ROWS_LIMIT:
            # df = q.collect(engine='streaming')

            # columns_date_parse = []
            # if TYPE_PROCESS_CSV > 1:
            #     for y in COLUMNS_DATE:
            #         parsed = False
            #         if not parsed:
            #             for fmt1 in FORMATS_DATE_FMT8:
            #                 for fmt2 in FORMATS_DATE_FMT10:
            #                     try:
            #                         df = df.with_columns(pl.col(y).str.slice(0, 10).str.strptime(pl.Date, format=fmt1).cast(pl.Date).alias(y))
            #                         columns_date_parse.append(y)
            #                         parsed = True
            #                         break
            #                     except:
            #                         try:
            #                             df = df.with_columns(pl.col(y).str.slice(0, 10).str.strptime(pl.Date, format=fmt2).cast(pl.Date).alias(y))
            #                             columns_date_parse.append(y)
            #                             parsed = True
            #                             break
            #                         except:
            #                             pass
            #                 if parsed:
            #                     break 
            #         else:
            #             break
            # else:
            #     columns_date_parse.clear()
            #     for y in COLUMNS_DATE:
            #         parsed = False
            #         if not parsed:
            #             for fmt1 in FORMATS_DATE_FMT8:
            #                 for fmt2 in FORMATS_DATE_FMT10:
            #                     try:
            #                         df = df.with_columns(
            #                             pl.when(pl.col(y).str.len_chars() == 8)
            #                             .then(pl.col(y).str.slice(0, 10).str.strptime(pl.Date, format=fmt1).cast(pl.Date))
            #                             .when(pl.col(y).str.len_chars() == 10)
            #                             .then(pl.col(y).str.slice(0, 10).str.strptime(pl.Date, format=fmt2).cast(pl.Date))
            #                             .otherwise((pl.col(y).str.slice(0, 10).str.strptime(pl.Date).cast(pl.Date))).alias(y),
            #                         ) 
            #                         columns_date_parse.append(y)
            #                         parsed = True
            #                         break
            #                     except:
            #                         pass
            #                 if parsed:
            #                     break 
            #         else:
            #             break

            # columns_date_no_parse = [column for column in COLUMNS_DATE if column not in columns_date_parse]
            # for y in columns_date_no_parse:
            #     df = df.with_columns(pl.col(y).str.slice(0, 10).str.strptime(pl.Date, strict=False).cast(pl.Date).alias(y))
            
            # q = df.lazy()
            def try_parse_date(col_name, formats):
                expressions = [
                    pl.col(col_name).str.slice(0, 10).str.to_date(fmt, strict=False) 
                    for fmt in formats
                ]
                return pl.coalesce(expressions)

            q = q.with_columns([
                try_parse_date(col, FORMATS_DATE).alias(col) 
                for col in COLUMNS_DATE
            ])
        else:
            # q = (
            #     q
            #     # .with_columns(pl.col('DNI_ASEGURADO').is_null().alias('ES_NULO_DNI'))
            #     # .with_columns(pl.col('RUC_ASEGURADO').is_null().alias('ES_NULO_RUC'))
            #     # .with_columns(pl.col('DNI_ASEGURADO').str.contains(r"^\d+$", literal=False).alias('ES_NUMERO_DNI'))
            #     # .with_columns(pl.col('RUC_ASEGURADO').str.contains(r"^\d+$", literal=False).alias('ES_NUMERO_RUC'))    
            #     # .with_columns(pl.col('F_EMISION_POLIZA').str.strptime(pl.Date, format='%d/%m/%Y', strict=False).cast(pl.Date))
            #     # .with_columns(pl.col('F_EMISION_CERT').str.strptime(pl.Date, format='%d/%m/%Y', strict=False).cast(pl.Date))
            #     # .with_columns(pl.col('F_INI_POLIZA').str.strptime(pl.Date, format='%d/%m/%Y', strict=False).cast(pl.Date))
            #     # .with_columns(pl.col('F_FIN_POLIZA').str.strptime(pl.Date, format='%d/%m/%Y', strict=False).cast(pl.Date))
            #     .with_columns(pl.col('F_ANULACION_CERT').str.slice(0, 10).str.to_date('%d/%m/%Y', strict=False)) #strptime(pl.Date, format='%d/%m/%Y').cast(pl.Date)
            #     .with_columns(pl.col('F_INI_COBER').str.slice(0, 10).str.to_date('%d/%m/%Y', strict=False))
            #     .with_columns(pl.col('F_FIN_COBERT').str.slice(0, 10).str.to_date('%d/%m/%Y', strict=False)) #to_date('%d/%m/%Y', strict=False)
            # ) 
            q = (
                q
                .with_columns(
                    pl.col(COLUMNS_DATE)
                    .str.slice(0, 10)
                    .str.to_date('%d/%m/%Y', strict=True) #.str.strptime(pl.Date).cast(pl.Date)) 
                )
            )

        dates_placeholder = [
            datetime.date(2103, 6, 19), 
            datetime.date(2200, 2, 3), 
            datetime.date(4016, 3, 1)
        ]

        q = (
            q
            .with_columns(
                pl.when(
                    pl.col('F_FIN_COBERT').is_in(dates_placeholder)
                )
                .then(None)
                .otherwise(pl.col('F_FIN_COBERT'))
                .alias('F_FIN_COBERT')
            )
        )
            
        # def date_valid(col, col_null=True):
        #     range_valid = (col >= pl.date(1900, 1, 1)) & (col <= pl.date(9000, 12, 31))
            
        #     if col_null:
        #         return col.is_null() | range_valid
        #     else:
        #         return range_valid

        # q = (
        #     q
        #     .filter(
        #         ( 
        #             date_valid(pl.col('F_INI_COBER'), False) &
        #             date_valid(pl.col('F_FIN_COBERT')) &
        #             date_valid(pl.col('F_ANULACION_CERT'))
        #         )
        #     )
        # )

        def date_valid_expr(col_name: str, allow_null: bool = True) -> pl.Expr:
            col = pl.col(col_name)
            range_valid = col.is_between(datetime.date(1900, 1, 1), datetime.date(9000, 12, 31), closed='both')
            
            if allow_null:
                return col.is_null() | range_valid
            else:
                return range_valid

        q = q.filter(
            date_valid_expr('F_INI_COBER', allow_null=False),
            date_valid_expr('F_FIN_COBERT'),
            date_valid_expr('F_ANULACION_CERT')
        )

        expr_start_date = (
            pl.when(
                (pl.col('F_INI_COBER').dt.day() == 1) | 
                ((pl.col('F_FIN_COBERT') == pl.col('F_INI_COBER')) & (pl.col('F_FIN_COBERT').is_not_null())) |
                (
                    pl.col('F_FIN_COBERT').is_not_null() 
                    & 
                    ((pl.col('F_FIN_COBERT') - pl.col('F_INI_COBER')).dt.total_days() <= 30) 
                    & 
                    (pl.col('F_INI_COBER').dt.month() >= pl.col('F_FIN_COBERT').dt.month())
                )
            )
            .then(pl.col('F_INI_COBER'))
            .otherwise(pl.col('F_INI_COBER').dt.offset_by('1mo').dt.truncate('1mo'))
        )

        expr_end_date = (
            pl.when(
                pl.col('F_FIN_COBERT').is_not_null() &
                (pl.col('F_FIN_COBERT') >= pl.col('F_INI_COBER')) &
                (pl.col('F_FIN_COBERT') <= pl.col('F_INI_COBER').dt.offset_by('85y'))
            )
            .then(pl.col('F_FIN_COBERT'))
            .otherwise(
                pl.when(pl.col('F_INI_COBER').dt.day() == 1)
                .then(pl.col('F_INI_COBER'))
                .otherwise(pl.col('F_INI_COBER').dt.offset_by('1mo').dt.truncate('1mo'))
            )
        )

        q = (
            q.with_columns(
                pl.date_ranges(
                    start=expr_start_date,
                    end=expr_end_date,
                    interval='1mo',
                    closed='both'
                ).alias('F_OCURRENCIA')
            )
            .explode('F_OCURRENCIA')
        )

        return (
            q
            .with_columns(pl.lit('CORE').alias('BASE'))
            .with_columns(pl.lit(datetime.date.today()).cast(pl.Date).alias('FECHA_REGISTRO')) 
            .select(COLUMNS_FINAL)
        )

    def Read_Excel_Siniestros(self, excel_path: Path, columns_names: list[str]) -> pl.LazyFrame | None:
        try:
            # squema_default = {clave: pl.Utf8 for clave in columns}
            # squema_final = squema_default.copy()
            # squema_final.update({"Inicio": pl.Date, "Fin": pl.Date,
            #                 "Tasa Comercial": pl.Float32, "Prima Calculada": pl.Float32,
            #                 "Diferencia": pl.Float32, "Prima Neta": pl.Float32,
            #                 "Igv": pl.Utf8, "Obs": pl.Utf8}) read_options={"schema": squema_default, "truncate_ragged_lines": True}
            
            # df_final = pl.LazyFrame(schema=squema_final)
            lf_final = []
            # workbook = load_workbook(excel_path, read_only=True) 
            # sheet_names = workbook.sheetnames
            # workbook.close()

            reader = fastexcel.read_excel(excel_path)
            dtypes_map = {col_name: "string" for col_name in columns_names}

            for name in reader.sheet_names:
                # try:
                #     q = (
                #         pl.read_excel(excel_path, 
                #                         sheet_name=name, 
                #                         # engine='xlsx2csv', 
                #                         # engine_options={'ignore_formats':['float']},
                #                         columns=columns_names,
                #                         infer_schema_length=0,
                #                         # read_options={'infer_schema': False, 'truncate_ragged_lines': True}
                #                     ).lazy()
                #         .with_columns([
                #                 pl.col(col).str.strip_chars() for col in columns_names
                #         ])
                #     )

                #     df = q.collect(engine='streaming')
                # except Exception as e:
                #     logger.warning(f"No se pudo leer contenido de la hoja '{name}', se omite.\nArchivo Excel : ./{excel_path.parent.name}/{excel_path.name}")
                #     continue

                try:
                    sheet = reader.load_sheet_by_name(name, use_columns=columns_names, dtypes=dtypes_map)
                    q = sheet.to_polars().lazy()
                except Exception:
                    logger.warning(f"No se pudo leer contenido de la hoja '{name}', se omite.\nArchivo Excel : ./{excel_path.parent.name}/{excel_path.name}")
                    continue
                
                # columns_date_parse = False
                # parsed = False
                # for fmt1 in FORMATS_DATE_FMT8:
                #     for fmt2 in FORMATS_DATE_FMT10:
                #         try:
                #             df = df.with_columns(
                #                 pl.when(pl.col('OCURRENCIA').str.len_chars() == 8)
                #                 .then(pl.col('OCURRENCIA').str.strptime(pl.Date, format=fmt1).cast(pl.Date))
                #                 .when(pl.col('OCURRENCIA').str.len_chars() == 10)
                #                 .then(pl.col('OCURRENCIA').str.strptime(pl.Date, format=fmt2).cast(pl.Date))
                #                 .otherwise((pl.col('OCURRENCIA').str.strptime(pl.Date).cast(pl.Date))).alias('OCURRENCIA'),
                #             ) 
                #             columns_date_parse = True
                #             parsed = True
                #             break
                #         except:
                #             pass
                #     if parsed:
                #         break 

                # if not columns_date_parse: 
                #     df = df.with_columns(pl.col('OCURRENCIA').str.slice(0, 10).str.strptime(pl.Date, strict=False).cast(pl.Date).alias('OCURRENCIA'))
                
                # df_lazy = df.lazy()
                # lf_final.append(df_lazy)
                lf_final.append(q)
                q.clear()
            
            lf: pl.LazyFrame = pl.concat(lf_final)
            n_rows = lf.limit(1).collect(engine='streaming').height
            return lf if n_rows > 0 else None      
        except Exception as e:
            raise Exception(f"{e}\nUbicación Archivo Excel: {excel_path}")

    def Transform_Dataframe_Siniestros(self,lf: pl.LazyFrame) -> pl.LazyFrame:
        global NUM_ROWS

        q = lf.with_columns([
                pl.col(col).str.strip_chars() for col in lf.collect_schema().names()
            ])

        def try_parse_date(col_name, formats):
            expressions = [
                pl.col(col_name).str.slice(0, 10).str.to_date(fmt, strict=False) 
                for fmt in formats
            ]
            return pl.coalesce(expressions)

        q = q.with_columns([
            try_parse_date(col, FORMATS_DATE).alias(col) 
            for col in ['OCURRENCIA']
        ])

        # def date_valid(col):
        #     return (    
        #             (
        #                 (col >= pl.date(1900, 1, 1)) & 
        #                 (col <= pl.date(9000, 12, 31)) & 
        #                 (col != pl.date(2103, 6, 19)) & 
        #                 (col != pl.date(2200, 2, 3)) & 
        #                 (col != pl.date(4016, 3, 1))
        #             )
        #         )

        # q = (
        #     q
        #     .filter(date_valid(pl.col('F_INI_COBER')))
        # )

        def date_valid(col_name: str):
            col = pl.col(col_name)
            is_range_ok = col.is_between(datetime.date(1900, 1, 1), datetime.date(9000, 12, 31), closed='both')
            is_not_bad = ~col.is_in([
                datetime.date(2103, 6, 19),
                datetime.date(2200, 2, 3),
                datetime.date(4016, 3, 1)
            ])
            
            return is_range_ok & is_not_bad

        q = (
            q
            # .filter( ~((pl.col('OCURRENCIA').is_null())))
            # .filter(~( (pl.col('OCURRENCIA').is_null()) | (pl.col('ASEGURADO').str.len_chars() < 3)
            #           | (pl.col('DNI').is_null()) | (pl.col('DNI').str.len_chars() < 3)))
            .filter(
                pl.col('OCURRENCIA').is_not_null(),
                pl.col('DNI').is_not_null(),

                date_valid('OCURRENCIA'),

                pl.col('DNI').str.len_chars() >= 3,
                pl.col('ASEGURADO').str.len_chars() >= 3
            )
            .with_columns(
                pl.when(
                    (pl.col('DNI').str.len_chars().is_in([5, 6, 7])) &
                    (pl.col('DNI').str.contains(r"^\d+$", literal=False))
                )
                .then(pl.col('DNI').str.zfill(8))
                # .when(pl.col('DNI').str.len_chars() < 5)
                # .then(pl.lit(None))
                .otherwise(pl.col('DNI'))
                .alias('DNI')
            )
            .rename({'RAMO': 'COD_RAMO', 'DNI': 'NRO_DOCUMENTO', 'OCURRENCIA': 'F_INI_COBER'})
            .with_columns(pl.lit('DNI').alias('TIPO_DOC')) 
            .with_columns(pl.lit('SINIESTROS').alias('BASE'))
            .with_columns(pl.lit(None).cast(pl.Date).alias('F_EMISION_POLIZA')) 
            .with_columns(pl.lit(None).cast(pl.Date).alias('F_EMISION_CERT')) 
            .with_columns(pl.lit(None).cast(pl.Date).alias('F_INI_POLIZA')) 
            .with_columns(pl.lit(None).cast(pl.Date).alias('F_FIN_POLIZA')) 
            .with_columns(pl.lit(None).cast(pl.Date).alias('F_ANULACION_CERT')) 
            .with_columns(pl.lit(None).cast(pl.Date).alias('F_FIN_COBERT')) 
            .with_columns(pl.lit(None).cast(pl.String).alias('PARENTESCO')) 
            .with_columns(pl.lit(None).cast(pl.String).alias('FI')) 
            .with_columns(pl.lit(datetime.date.today()).cast(pl.Date).alias('FECHA_REGISTRO'))
        )

        expr_start_date = (
            pl.when(
                (pl.col('F_INI_COBER').dt.day() == 1) | 
                ((pl.col('F_FIN_COBERT') == pl.col('F_INI_COBER')) & (pl.col('F_FIN_COBERT').is_not_null())) |
                (
                    pl.col('F_FIN_COBERT').is_not_null() 
                    & 
                    ((pl.col('F_FIN_COBERT') - pl.col('F_INI_COBER')).dt.total_days() <= 30) 
                    & 
                    (pl.col('F_INI_COBER').dt.month() >= pl.col('F_FIN_COBERT').dt.month())
                )
            )
            .then(pl.col('F_INI_COBER'))
            .otherwise(pl.col('F_INI_COBER').dt.offset_by('1mo').dt.truncate('1mo'))
        )

        expr_end_date = (
            pl.when(
                pl.col('F_FIN_COBERT').is_not_null() &
                (pl.col('F_FIN_COBERT') >= pl.col('F_INI_COBER')) &
                (pl.col('F_FIN_COBERT') <= pl.col('F_INI_COBER').dt.offset_by('85y'))
            )
            .then(pl.col('F_FIN_COBERT'))
            .otherwise(
                pl.when(pl.col('F_INI_COBER').dt.day() == 1)
                .then(pl.col('F_INI_COBER'))
                .otherwise(pl.col('F_INI_COBER').dt.offset_by('1mo').dt.truncate('1mo'))
            )
        )

        q = (
            q.with_columns(
                pl.date_ranges(
                    start=expr_start_date,
                    end=expr_end_date,
                    interval='1mo',
                    closed="both"
                ).alias('F_OCURRENCIA')
            )
            .explode('F_OCURRENCIA')
        )

        q = (
            q
            .unique(subset=COLUMNS_FINAL)
            .select(COLUMNS_FINAL)
        )

        NUM_ROWS = q.select(pl.len()).collect(engine='streaming').item()
        logger.info("Transformando datos de carpeta Siniestros...")
        return q

    def Export_Final_Report(self, process_name: str, lf: pl.LazyFrame, report_name: Path):
        global FILES_TEMP_REMOVE

        path_prev = Path(tempfile.gettempdir()) / report_name
        path_report = PATH_DESTINATION / report_name
        
        logger.info(lf.collect_schema())
        # print(f"Total de Registros : {lf.select(pl.len()).collect(engine='streaming').item()}")
        print(lf.select(['COD_RAMO','ASEGURADO','CERTIFICADO','NRO_DOCUMENTO',
          'F_INI_COBER','F_FIN_COBERT','F_OCURRENCIA']).limit(20).collect())
        # print(lf.select(['COD_RAMO','ASEGURADO','CERTIFICADO','NRO_DOCUMENTO',
        #  'F_INI_COBER','F_FIN_COBERT','F_OCURRENCIA']).filter(pl.col('CERTIFICADO') == '1000751549').limit(20).collect().head(20))
        # print(q
        #       .unique(subset=['ASEGURADO','TIPO_DOC','NRO_DOCUMENTO'])
        #       .select(['ASEGURADO','TIPO_DOC','NRO_DOCUMENTO'])
        #       .filter((pl.col('NRO_DOCUMENTO').str.contains(r"^[a-zA-ZñÑáéíóúÁÉÍÓÚ\s]+$", literal=False)))
        #       .collect()
        #       .head(100)
        # )
        # q = None

        logger.info(f'Verificando si existe consolidado {process_name}...')
        FILES_TEMP_REMOVE.append(path_prev)
        self.Delete_Temp_Files(FILES_TEMP_REMOVE)

        # FILES_TEMP_REMOVE.clear()

        # first_df = lf.limit(1).collect(engine='streaming').to_arrow() 
        # arrow_schema = first_df.schema
        # print(arrow_schema)
        # print(first_df.num_rows)

        # def write_dataframe_to_pyarrow(row: int):
        #     global FILES_TEMP_REMOVE

        #     path_prev = Path(tempfile.gettempdir()) / f'Consolidado_Emision_Core_{row}_{PERIODO}.parquet' 
        #     FILES_TEMP_REMOVE.append(path_prev)
        #     writer = pq.ParquetWriter(
        #         where=path_prev,
        #         schema=arrow_schema,
        #         compression="ZSTD",
        #         compression_level=20,
        #         use_dictionary=False,
        #         write_statistics=False 
        #     )
        #     df_chunk = lf.slice(row, chunk_size).collect(engine='streaming').to_arrow()
        #     writer.write_table(df_chunk, row_group_size=5_000_000)
        #     writer.close()

        #     return df_chunk.num_rows

        # with Progress(
        #     TextColumn("[bold blue]{task.description}"),
        #     BarColumn(),
        #     TaskProgressColumn(),
        #     TimeElapsedColumn(),
        #     TimeRemainingColumn(),
        #     transient=True
        # ) as progress:
        #     task = progress.add_task("Exportando a Parquet...", total=NUM_ROWS)

        #     min_chunk = 1_000_000
        #     chunk_size = max(math.ceil(0.2 * NUM_ROWS), min_chunk)

        #     with ThreadPoolExecutor(max_workers=5) as executor:
        #         futures = {executor.submit(write_dataframe_to_pyarrow, i): i for i in range(0, NUM_ROWS, chunk_size)}
        #         for future in as_completed(futures):
        #             num_row = future.result()
        #             progress.update(task, advance=num_row)

        logger.info(f'Generando archivo final {process_name}...')
        lf.sink_parquet(path_prev, 
            compression = 'zstd', 
            compression_level = 3, 
            row_group_size = 1 * 1_000_000,
            statistics = True
        )
        lf.clear()
        # print(FILES_TEMP_REMOVE)
        # self.Delete_Temp_Files(FILES_TEMP_REMOVE)

        logger.info(f'Guardando archivo final {process_name}...')
        if os.path.exists(path_prev):
            with suppress(FileNotFoundError):
                path_report.unlink(missing_ok=True)
                shutil.move(path_prev,path_report)

    def Process_Start(self):
        global HORA_INICIAL, HORA_FINAL

        HORA_INICIAL = datetime.datetime.now()

        # print('Hora de Inicio: ' + datetime.datetime.strftime(HORA_INICIAL,'%d/%m/%Y %H:%M:%S'))
        # console.print()
        # console.rule(characters="─", style="grey30")
        # Mostrar el título de inicio del proceso
        nombres = {"1": "Cargar Base Core", "2": "Cargar Base Siniestros", "3": "Cargar Ambas Bases (Core/Siniestros)"}
        nombre_proceso = nombres.get(str(self.process).strip(), "Proceso Desconocido")
        console.rule(f"[grey66]Proceso Iniciado: [bold white]{nombre_proceso}[/bold white][/grey66]")
        remove_log()
        if FILE_LOG_EXISTS:
            PATH_LOG.unlink(missing_ok=True)
        add_log_file(False)
        logger.info(f'Comienzo del Proceso {nombre_proceso}...')
        remove_log()
        start_log(True)
        try:
            if self.process == 1 or self.process == 3:
                if not PATH_SOURCE_CORE.exists():
                    raise FileNotFoundError(f"La carpeta principal no existe o tiene un nombre diferente de 'Reportes_Core'.\nUbicación Carpeta Esperada: {PATH_SOURCE_CORE}")
            
            if self.process == 2 or self.process == 3:
                if not PATH_SOURCE_SNTROS.exists():
                    raise FileNotFoundError(f"La carpeta principal no existe o tiene un nombre diferente de 'Reportes_Siniestros'.\nUbicación Carpeta Esperada: {PATH_SOURCE_SNTROS}")

            if not PATH_DESTINATION.exists():
                raise FileNotFoundError(f"La carpeta destino no existe..\nUbicación Carpeta Esperada: {PATH_DESTINATION}")            
                
            if self.process == 1 or self.process == 3:
                # lazyframes_folder_core = []
                # lazyframes = []
                # conteo_total_csv = 0
                logger.info('Obteniendo SubCarpetas Core...')
                subfolders_list = [c for c in PATH_SOURCE_CORE.iterdir() if c.is_dir()]
                if not subfolders_list:
                    raise Exception(f"No se encontraron subcarpetas en la carpeta principal.\nUbicación Carpeta Core: {PATH_SOURCE_CORE}")
                
                def processing_csvs_core(csv_files_list: list[Path]):
                    for csv in csv_files_list:
                        lf = self.Read_CSV_Core(csv, COLUMNS_CORE)
                        if lf is None:
                            # logger.warning(f"Archivo sin datos, se omite: {csv}")
                            # continue
                            raise Exception(f"El archivo CSV no cuenta con información.\nUbicación Archivo CSV: {csv}")
                        yield lf

                def processing_subfolders_core(subfolders_list: list[Path]):
                    logger.info('Recorriendo contenido de SubCarpetas Core...')
                    for folder in subfolders_list:
                        csvs = list(folder.glob("*.csv"))
                        if not csvs:
                            logger.warning(f'No se encontraron archivos CSVs en subcarpeta, se omite.\nUbicación Subcarpeta : {folder}')
                            continue
                                
                        # for csv in csvs:
                        #     lf = self.Read_CSV_Core(csv, COLUMNS_CORE)
                        #     # q = lf.filter(pl.col('DNI_ASEGURADO') == '73428207') 
                        #     # n = q.select(['ID', 'DNI_ASEGURADO']).limit(20).collect().head(20)
                        #     # if(n.height > 0):
                        #     #     print(csv)
                        #     #     print(n) 
                        #     #     sys.exit()                
                        #     if lf is None:
                        #         raise Exception(f"El archivo CSV no cuenta con información.\nUbicación Archivo CSV: {csv}")
                        #     lazyframes.append(lf)
                        #     conteo_total_csv += 1
                        # csvs = []
                        # print('Consolidando información...')
                        # df_lazy_prev = pl.concat(lazyframes)

                        # print('Transformando columnas...'))
                        # lf_prev_list: pl.LazyFrame = pl.concat(processing_csvs_core(csvs))
                        # if lf_prev_list is None:
                        #     raise Exception(f"No se pudo leer el contenido de ningún archivo CSV.\nUbicación SubCarpeta : {carpeta}")
                        
                        q = self.Transform_Dataframe_Core(pl.concat(processing_csvs_core(csvs)), folder)
                        csvs.clear()
                        yield q
                        # lazyframes = []
                        # print(q.collect_schema())

                        # lazyframes_folder_core.append(q)
                        # q = None

                        # print(q.collect_schema()) 
                        # print(f"Total de Registros : {q.select(pl.len()).collect(engine='streaming').item()}")
                        # print(q.select(['ASEGURADO','TIPO_DOC','NRO_DOCUMENTO',
                        # 'F_INI_COBER','F_FIN_COBERT','F_ANULACION_CERT']).sort(by='ASEGURADO').limit(20).collect().head(20))
                        # print(q
                        #       .unique(subset=['ASEGURADO','TIPO_DOC','NRO_DOCUMENTO'])
                        #       .select(['ASEGURADO','TIPO_DOC','NRO_DOCUMENTO'])
                        #       .filter((pl.col('NRO_DOCUMENTO').str.contains(r"^[a-zA-ZñÑáéíóúÁÉÍÓÚ\s]+$", literal=False)))
                        #       .collect()
                        #       .head(100)
                        # )
                        # q = None
                
                lf_final = pl.concat(processing_subfolders_core(subfolders_list))
                # print(lf_final)

                n_row = int(lf_final.select(pl.len()).collect(engine='streaming').item())
                if n_row == 0:
                    raise Exception(f"No se encontraron registros en la carpeta Core.")

                logger.info('Consolidando información Core...')
                logger.info(f"Total Registros: {n_row}...")
                self.Export_Final_Report('Core', lf_final, REPORT_NAME_CORE)

                # self.Export_Final_Report('Core', lazyframes_folder_core, REPORT_NAME_CORE, REPORT_NAME_CORE)
                # lazyframes_folder_core = None
                
            if self.process == 2 or self.process == 3:
                # lazyframes_folder_sntros = []
                # lazyframes = []
                logger.info('Recorriendo contenido de carpeta Siniestros...')

                # excels = list(PATH_SOURCE_SNTROS.glob("*.xlsx")) + list(PATH_SOURCE_SNTROS.glob("*.xls"))
                excels = [f for f in PATH_SOURCE_SNTROS.iterdir() if f.suffix in ['.xlsx','.xls']]
                if not excels:
                    raise Exception(f"No se encontraron archivos Excel en carpeta principal.\nUbicación Carpeta Siniestros: {PATH_SOURCE_SNTROS}")
                    
                def processing_excels_sntros(excels_files_list: list[Path]):
                    for excel in excels_files_list:
                        lf = self.Read_Excel_Siniestros(excel, COLUMNS_SNTROS)           
                        if lf is None:
                            raise Exception(f"El archivo Excel no cuenta con información.\nUbicación Archivo Excel: {excel}")
                        yield lf

                lf_final = self.Transform_Dataframe_Siniestros(pl.concat(processing_excels_sntros(excels)))

                # lazyframes_folder_sntros.append(q)
                # del q

                n_row = int(lf_final.select(pl.len()).collect(engine='streaming').item())
                if n_row == 0:
                    raise Exception(f"No se encontraron registros en la carpeta Siniestros.")

                logger.info('Consolidando información Siniestros...')
                logger.info(f"Total Registros: {n_row}...")
                self.Export_Final_Report('Siniestros', lf_final, REPORT_NAME_SNTROS)
                # lazyframes_folder_sntros = None

                # print(q.collect_schema()) 113381 106361
                # print(q.select(pl.len()).collect(engine='streaming').item())
                # print(q.select(['ASEGURADO','TIPO_DOC','NRO_DOCUMENTO',
                # 'F_INI_COBER','F_FIN_COBERT','F_ANULACION_CERT']).sort(by='ASEGURADO').limit(20).collect().head(20))
                # q = q.select(['COD_RAMO','POLIZA','CERTIFICADO','ASEGURADO','TIPO_DOC','NRO_DOCUMENTO',
                #  'F_INI_COBER','F_FIN_COBERT','F_ANULACION_CERT']).sort(by='NRO_DOCUMENTO').collect()
                # print(q.filter(q.is_duplicated()).head(100))
                 # q = None

            HORA_FINAL = datetime.datetime.now()
            logger.success('Ejecución exitosa: Se cargó la información.')
            difference_time = HORA_FINAL-HORA_INICIAL
            total_seconds = int(difference_time.total_seconds())
            difference_formated = "{} minuto(s), {} segundo(s)".format((total_seconds // 60), total_seconds % 60)

            remove_log()
            add_log_file(True)
            logger.info(f'Tiempo de proceso: {difference_formated}')
            add_log_console()
            print(f'[dark_orange]Tiempo de proceso: {difference_formated}[/dark_orange]')

            console.rule("[grey66]Proceso Finalizado[/grey66]")
            print("[grey66]Presiona Enter para salir...[/grey66]")
            input()
            sys.exit(0)
        except Exception as e:
            self.Delete_Temp_Files(FILES_TEMP_REMOVE)
            # logger.info('Fin del Proceso...')
            HORA_FINAL = datetime.datetime.now()
            logger.error('Proceso Incompleto. Detalle: '+str(e))
            # print("Hora de Fin: "+datetime.datetime.strftime(HORA_FINAL,"%d/%m/%Y %H:%M:%S"))
            difference_time = HORA_FINAL-HORA_INICIAL
            total_seconds = int(difference_time.total_seconds())
            difference_formated = "{} minuto(s), {} segundo(s)".format((total_seconds // 60), total_seconds % 60)

            remove_log()
            add_log_file(True)
            logger.info(f'Tiempo de proceso: {difference_formated}')
            add_log_console()
            print(f'[dark_orange]Tiempo de proceso: {difference_formated}[/dark_orange]')

            show_custom_rule('Proceso Finalizado con Error', state='Error')
            # print('Ejecución con Error: '+str(e))
            # input('Presiona Enter para salir...')
            print("[grey66]Presiona Enter para salir...[/grey66]")
            input()
            sys.exit(1)

if __name__=='__main__':
    start_log()
    console = Console()
    menu_text = (
        "[bold grey93]\nSeleccione el tipo de Proceso[/bold grey93]\n\n"
        "[cyan]1.[/] Cargar Base Core\n"
        "[cyan]2.[/] Cargar Base Siniestros\n"
        "[cyan]3.[/] Cargar Ambas Bases (Core/Siniestros)\n"
    )
    console.print(Panel.fit(menu_text, title="[bold]Menú de Procesos[/bold]", border_style="grey50"))

    process_type = MenuPrompt.ask(
        "[bold white]Escriba el Nro de opción[/bold white]", 
        choices=["1", "2", "3"]
    )

    Process_ETL(process_type)


