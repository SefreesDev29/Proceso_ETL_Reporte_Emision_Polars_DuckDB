from pathlib import Path
from loguru import logger
from rich import print
from rich.console import Console
from rich.panel import Panel
from rich.prompt import IntPrompt
from rich.text import Text
from contextlib import suppress
import polars as pl
import datetime
import os, sys, shutil, tempfile

# pyinstaller --noconfirm --onefile Carga_Reportes_Emision_BK.py --icon "Recursos/logo.ico"
# --hidden-import pyarrow.vendored.version

HORA_INICIAL, HORA_FINAL = datetime.datetime.now(), datetime.datetime.now()
PERIODO = str(HORA_INICIAL.year) + str(HORA_INICIAL.month).zfill(2) + str(HORA_INICIAL.day).zfill(2)
if getattr(sys, 'frozen', False): 
    PATH_GENERAL = Path(sys.executable).resolve().parent  #sys.argv[1]
else:
    PATH_GENERAL = Path(__file__).resolve().parent  #sys.argv[1]
PATH_DESTINATION =  PATH_GENERAL / 'Consolidados'
PATH_LOG = PATH_GENERAL / 'Logs' / f'LogApp_{PERIODO}.log'
FILE_LOG_EXISTS = False
REPORT_NAME_REPROCESS = f'Consolidado_Emision_{PERIODO}_Reproceso.parquet' 
FILES_TEMP_REMOVE = []
COLUMNS_TRANS = ['COD_RAMO','POLIZA','CERTIFICADO','ASEGURADO','TIPO_DOC','NRO_DOCUMENTO',
                'F_INI_COBER','F_FIN_COBERT','F_ANULACION_CERT','F_OCURRENCIA',
                'PARENTESCO', 'FI', 'FECHA_REGISTRO','BASE']
COLUMNS_FINAL = ['COD_RAMO','POLIZA','CERTIFICADO','ASEGURADO','TIPO_DOC','NRO_DOCUMENTO','ULT_DIGI_DOC',
                'F_INI_COBER','F_FIN_COBERT','F_ANULACION_CERT','F_OCURRENCIA',
                'PARENTESCO', 'FI', 'FECHA_REGISTRO','BASE']

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
            if self.process not in [1]:
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
                
    def Read_Parquet(self, parquet_path: Path, columns: list[str]) -> pl.LazyFrame:
        try:
            lf = pl.scan_parquet(parquet_path)

            originales = lf.collect_schema().names()
            if len(originales) != len(columns):
                raise ValueError(f"Cantidad de columnas incorrecta. Permitido: {len(columns)}")

            n_rows = lf.select(columns).limit(1).collect(engine='streaming').height

            return lf if n_rows > 0 else None
        except Exception as e:
            raise Exception(f"{e}\nArchivo Parquet: ./{parquet_path.parent.name}/{parquet_path.name}") from e

    def Transform_Dataframe(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        q = (
            lf
            .with_columns(pl.col('NRO_DOCUMENTO').str.slice(-1).cast(pl.Int8, strict=False).alias('ULT_DIGI_DOC'))
        )

        return (
            q
            .select(COLUMNS_FINAL)
        )

    def Export_Final_Report(self, lf: pl.LazyFrame, report_name: str):
        global FILES_TEMP_REMOVE

        path_prev = Path(tempfile.gettempdir()) / report_name
        path_report = PATH_DESTINATION / report_name

        logger.info(f'Consolidando información...')
        q: pl.LazyFrame = lf
        logger.info(f"Total Registros: {q.select(pl.len()).collect(engine='streaming').item()}...")
        
        logger.info(q.collect_schema())

        logger.info(f'Verificando si existe consolidado...')
        FILES_TEMP_REMOVE.append(path_prev)
        self.Delete_Temp_Files(FILES_TEMP_REMOVE)

        q.sink_parquet(path_prev, 
            compression = 'zstd', 
            compression_level = 3, 
            row_group_size = 1 * 1_000_000,
            statistics = True
        )

        logger.info(f'Guardando archivo final...')
        if os.path.exists(path_prev):
            with suppress(FileNotFoundError):
                path_report.unlink(missing_ok=True)
                shutil.move(path_prev,path_report)

    def Process_Start(self):
        global HORA_INICIAL, HORA_FINAL

        HORA_INICIAL = datetime.datetime.now()

        nombres = {"1": "Reprocesar Archivos Emisión"}
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
            if not PATH_DESTINATION.exists():
                raise FileNotFoundError(f"La carpeta destino no existe..\nUbicación Carpeta Esperada: {PATH_DESTINATION}")            
                
            logger.info('Obteniendo SubArchivos Parquet...')
            parquets_files = list(PATH_DESTINATION.glob("*.parquet"))
            if not parquets_files:
                raise Exception(f"No se encontraron SubArchivos Parquet en la carpeta principal.\nUbicación Carpeta: {PATH_DESTINATION}")
            
            def processing_parquets(parquets_files: list[Path]):
                global REPORT_NAME_REPROCESS
                for parquet in parquets_files:
                    REPORT_NAME_REPROCESS = f'{parquet.stem}_Reproceso.parquet' 
                    logger.info(f'Leyendo SubArchivo Parquet {parquet.name}...')
                    lf = self.Read_Parquet(parquet, COLUMNS_TRANS)
                    if lf is None:
                        raise Exception(f"El archivo parquet no cuenta con información.\nUbicación Archivo Parquet: {parquet}")
                    yield lf
            
            lf_final = self.Transform_Dataframe(pl.concat(processing_parquets(parquets_files)))

            self.Export_Final_Report(lf_final, REPORT_NAME_REPROCESS)

            HORA_FINAL = datetime.datetime.now()
            logger.success('Ejecución exitosa: Se reprocesó la información.')
            difference_time = HORA_FINAL-HORA_INICIAL
            total_seconds = int(difference_time.total_seconds())
            difference_formated = "{} minuto(s), {} segundo(s)".format((total_seconds // 60) % 60, total_seconds % 60)

            remove_log()
            add_log_file(True)
            logger.info(f'Tiempo de proceso: {difference_formated}')
            add_log_console()
            print(f'[dark_orange]Tiempo de proceso: {difference_formated}[/dark_orange]')

            console.rule(f"[grey66]Proceso Finalizado[/grey66]")
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
        "[cyan]1.[/] Reprocesar Archivo Parquet\n"
    )
    console.print(Panel.fit(menu_text, title="[bold]Menú de Procesos[/bold]", border_style="grey50"))
    
    process_type = MenuPrompt.ask(
        "[bold white]Escriba el Nro de opción[/bold white]", 
        choices=["1", "2", "3"]
    )

    Process_ETL(process_type)


