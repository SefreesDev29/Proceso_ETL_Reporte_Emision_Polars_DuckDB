from pathlib import Path
from loguru import logger
from rich import print
from rich.console import Console
from rich.panel import Panel
from rich.prompt import IntPrompt
from rich.text import Text
from contextlib import suppress
import polars as pl
import fastexcel
import datetime
from charset_normalizer import from_bytes
import os, sys, shutil, tempfile

# uv run pyinstaller --noconfirm --onefile --strip --icon "Recursos/logo.ico" --hidden-import fastexcel Carga_Reportes_Emision_V3.py 
# uv run pyinstaller --noconfirm --onedir --noupx --strip --icon "Recursos/logo.ico" --hidden-import fastexcel Carga_Reportes_Emision_V3.py 
#--clean --log-level=DEBUG 

HORA_INICIAL, HORA_FINAL = datetime.datetime.now(), datetime.datetime.now()
PERIODO = str(HORA_INICIAL.year) + str(HORA_INICIAL.month).zfill(2) + str(HORA_INICIAL.day).zfill(2)
if getattr(sys, 'frozen', False): 
    PATH_GENERAL = Path(sys.executable).resolve().parent  
else:
    PATH_GENERAL = Path(__file__).resolve().parent 
PATH_SOURCE_CORE = PATH_GENERAL / 'Reportes_Core' 
PATH_SOURCE_SNTROS = PATH_GENERAL / 'Reportes_Siniestros' 
PATH_DESTINATION =  PATH_GENERAL / 'Consolidados'
PATH_LOG = PATH_GENERAL / 'Logs' / f'LogApp_{PERIODO}.log'
FILE_LOG_EXISTS = False
REPORT_NAME_CORE = f'Consolidado_Emision_Core_{PERIODO}.parquet' 
REPORT_NAME_SNTROS = f'Consolidado_Emision_Siniestros_{PERIODO}.parquet' 
TYPE_PROCESS_CSV = 0
FILES_TEMP_REMOVE = []
COLUMNS_CORE = ['ID','COD_RAMO','RAMO','PRODUCTO','POLIZA','CERTIFICADO','TIPO','CONTRATANTE','ASEGURADO',
                'DNI_ASEGURADO','RUC_ASEGURADO','F_EMISION_POLIZA','F_EMISION_CERT','F_INI_POLIZA',
                'F_FIN_POLIZA','F_ANULACION_CERT','F_INI_COBER','F_FIN_COBERT','PARENTESCO','FI']
COLUMNS_STRUCT = ['CONTRATANTE','ASEGURADO','DNI_ASEGURADO','RUC_ASEGURADO','F_EMISION_POLIZA',
                'F_EMISION_CERT','F_INI_POLIZA','F_FIN_POLIZA','F_ANULACION_CERT','F_INI_COBER','F_FIN_COBERT','PARENTESCO','FI']
COLUMNS_DATE = ['F_ANULACION_CERT','F_INI_COBER','F_FIN_COBERT']
COLUMNS_SNTROS = ['DNI','RAMO','POLIZA','CERTIFICADO','ASEGURADO','OCURRENCIA']

COLUMNS_FINAL = ['COD_RAMO','POLIZA','CERTIFICADO','ASEGURADO','TIPO_DOC','NRO_DOCUMENTO',
                'F_INI_COBER','F_FIN_COBERT','F_ANULACION_CERT','F_OCURRENCIA','PARENTESCO','FI','FECHA_REGISTRO','BASE']
FORMATS_DATE = ["%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%Y/%m/%d", 
                "%d/%m/%y", "%d-%m-%y", "%y/%m/%d", "%y-%m-%d"]
ROWS_LIMIT = 10_000_000
NUM_ROWS = 0

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

                shutil.move(temp_file.name, path)

                return True
            except Exception:
                return None

        def try_read_lazy(path: Path, encoding: str) -> pl.LazyFrame | None:
            global TYPE_PROCESS_CSV
            try:
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
                    TYPE_PROCESS_CSV = 2
                    return pl.scan_csv(path, infer_schema=False, truncate_ragged_lines=True) 
                except Exception:
                    TYPE_PROCESS_CSV = 3
                    df = pl.read_csv(path, infer_schema=False, truncate_ragged_lines=True)
                    lf = df.lazy()
                    df.clear()
                    del df
                    return lf

        ct_encoding = self.Detect_encoding(csv_path)

        try:
            lf = try_read_lazy(csv_path, ct_encoding)
            originales = lf.collect_schema().names()

            if len(originales) != len(columns):
                raise ValueError(f"Cantidad de columnas incorrecta. Permitido: {len(columns)}")

            mapping = dict(zip(originales, columns))
            lf = lf.rename(mapping)

            if TYPE_PROCESS_CSV > 1:
                n_rows = lf.select(columns).limit(1).collect(engine='streaming').height
            else:
                n_rows = 1

            return lf if n_rows > 0 else None
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

        logger.info(f"Transformando datos de subcarpeta '{subfolder_path.name}'...")
        if NUM_ROWS < ROWS_LIMIT:
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
            q = (
                q
                .with_columns(
                    pl.col(COLUMNS_DATE)
                    .str.slice(0, 10)
                    .str.to_date('%d/%m/%Y', strict=True)
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
            lf_final = []

            reader = fastexcel.read_excel(excel_path)
            dtypes_map = {col_name: "string" for col_name in columns_names}

            for name in reader.sheet_names:
                try:
                    sheet = reader.load_sheet_by_name(name, use_columns=columns_names, dtypes=dtypes_map)
                    q = sheet.to_polars().lazy()
                except Exception:
                    logger.warning(f"No se pudo leer contenido de la hoja '{name}', se omite.\nArchivo Excel : ./{excel_path.parent.name}/{excel_path.name}")
                    continue
                
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

        logger.info(f"Total Registros: {lf.select(pl.len()).collect(engine='streaming').item()}...")
        
        logger.info(lf.collect_schema())

        logger.info(f'Verificando si existe consolidado {process_name}...')
        FILES_TEMP_REMOVE.append(path_prev)
        self.Delete_Temp_Files(FILES_TEMP_REMOVE)

        logger.info(f'Generando archivo final {process_name}...')
        lf.sink_parquet(path_prev, 
            compression = 'zstd', 
            compression_level = 3, 
            row_group_size = 1 * 1_000_000,
            statistics = True
        )
        lf.clear()

        logger.info(f'Guardando archivo final {process_name}...')
        if os.path.exists(path_prev):
            with suppress(FileNotFoundError):
                path_report.unlink(missing_ok=True)
                shutil.move(path_prev,path_report)

    def Process_Start(self):
        global HORA_INICIAL, HORA_FINAL

        HORA_INICIAL = datetime.datetime.now()

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
                logger.info('Obteniendo SubCarpetas Core...')
                subfolders_list = [c for c in PATH_SOURCE_CORE.iterdir() if c.is_dir()]
                if not subfolders_list:
                    raise Exception(f"No se encontraron subcarpetas en la carpeta principal.\nUbicación Carpeta Core: {PATH_SOURCE_CORE}")
                
                def processing_csvs_core(csv_files_list: list[Path]):
                    for csv in csv_files_list:
                        lf = self.Read_CSV_Core(csv, COLUMNS_CORE)
                        if lf is None:
                            raise Exception(f"El archivo CSV no cuenta con información.\nUbicación Archivo CSV: {csv}")
                        yield lf

                def processing_subfolders_core(subfolders_list: list[Path]):
                    logger.info('Recorriendo contenido de SubCarpetas Core...')
                    for folder in subfolders_list:
                        csvs = list(folder.glob("*.csv"))
                        if not csvs:
                            logger.warning(f'No se encontraron archivos CSVs en subcarpeta, se omite.\nUbicación Subcarpeta : {folder}')
                            continue
                        
                        q = self.Transform_Dataframe_Core(pl.concat(processing_csvs_core(csvs)), folder)
                        csvs.clear()
                        yield q
                
                lf_final = pl.concat(processing_subfolders_core(subfolders_list))

                logger.info('Consolidando información Core...')
                self.Export_Final_Report('Core', lf_final, REPORT_NAME_CORE)
                
            if self.process == 2 or self.process == 3:
                logger.info('Recorriendo contenido de carpeta Siniestros...')

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

                logger.info('Consolidando información Siniestros...')
                self.Export_Final_Report('Siniestros', lf_final, REPORT_NAME_SNTROS)

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
            HORA_FINAL = datetime.datetime.now()
            logger.error('Proceso Incompleto. Detalle: '+str(e))
            difference_time = HORA_FINAL-HORA_INICIAL
            total_seconds = int(difference_time.total_seconds())
            difference_formated = "{} minuto(s), {} segundo(s)".format((total_seconds // 60), total_seconds % 60)

            remove_log()
            add_log_file(True)
            logger.info(f'Tiempo de proceso: {difference_formated}')
            add_log_console()
            print(f'[dark_orange]Tiempo de proceso: {difference_formated}[/dark_orange]')

            show_custom_rule('Proceso Finalizado con Error', state='Error')
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


