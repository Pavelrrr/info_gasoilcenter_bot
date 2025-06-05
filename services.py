import os
import logging
import asyncio
import tempfile
import ydb
import httpx
import base64
import re
from dotenv import load_dotenv
from aiogoogle import Aiogoogle
from aiogoogle.auth.creds import ServiceAccountCreds
from utils import download_file
from datetime import date
load_dotenv()


logger = logging.getLogger(__name__)


# # Конфигурация Google Sheets
# CREDS_URL = os.environ.get("CREDS_URL")
# SHEET_NAMES = {
#     "drilling": "08:00",
#     "completion": "08:00 ОСВ"
# }

# Конфигурация YDB
YDB_ENDPOINT = os.environ.get("YDB_ENDPOINT")
YDB_DATABASE = os.environ.get("YDB_DATABASE")
YDB_KEY_SA = os.environ.get("YDB_KEY_SA")

# Глобальные переменные
_creds_dict = None
_ydb_key_path = None
ydb_driver = None
ydb_pool = None

async def get_ydb_key_path():
    """
    Возвращает путь к сервисному ключу YDB:
    - если есть переменная YDB_KEY_SA — сохраняет её содержимое во временный файл;
    - если нет, но есть YDB_KEY_SA_URL — скачивает ключ по ссылке и сохраняет во временный файл.
    """
    global _ydb_key_path
    if _ydb_key_path is not None:
        return _ydb_key_path

    key_json = os.environ.get("YDB_KEY_SA")
    if not key_json:
        raise ValueError("YDB_KEY_SA not set in environment variables")
    key_json = base64.b64decode(key_json).decode("utf-8")
    temp_file = tempfile.NamedTemporaryFile(mode='w', delete=False)
    temp_file.write(key_json)
    temp_file.close()
    return temp_file.name

    key_url = os.environ.get("YDB_KEY_SA_URL")
    if key_url:
        async with httpx.AsyncClient() as client:
            response = await client.get(key_url)
            response.raise_for_status()
            key_json = response.text
        temp_file = tempfile.NamedTemporaryFile(mode='w', delete=False)
        temp_file.write(key_json)
        temp_file.close()
        _ydb_key_path = temp_file.name
        return _ydb_key_path

    raise ValueError("YDB_SA_KEY_JSON or YDB_KEY_URL must be set in environment variables")

async def get_ydb_pool():
    """Инициализирует YDB драйвер и пул сессий"""
    global ydb_driver, ydb_pool
    if ydb_pool is None:
        try:
            # Получаем путь к ключу асинхронно
            key_path = await get_ydb_key_path()
            
            # Проверяем, что файл существует и не пустой
            if not os.path.exists(key_path):
                raise FileNotFoundError(f"YDB key file not found at {key_path}")
                
            with open(key_path, 'r') as f:
                content = f.read()
                if not content:
                    raise ValueError("YDB key file is empty")
                logger.info(f"YDB key file size: {len(content)} characters")
            
            # Проверяем переменные окружения
            if not YDB_ENDPOINT:
                raise ValueError("YDB_ENDPOINT not set")
            if not YDB_DATABASE:
                raise ValueError("YDB_DATABASE not set")
                
            logger.info(f"YDB_ENDPOINT: {YDB_ENDPOINT}")
            logger.info(f"YDB_DATABASE: {YDB_DATABASE}")
            
            # Инициализируем драйвер
            logger.info("Creating YDB driver...")
            ydb_driver = ydb.Driver(
                endpoint=YDB_ENDPOINT,
                database=YDB_DATABASE,
                credentials=ydb.iam.ServiceAccountCredentials.from_file(key_path)
            )
            
            logger.info("Waiting for YDB driver connection...")
            ydb_driver.wait(timeout=10)  # Увеличиваем timeout
            
            logger.info("Creating session pool...")
            ydb_pool = ydb.SessionPool(ydb_driver)
            logger.info("YDB pool initialized successfully")
            
        except Exception as e:
            logger.error(f"YDB initialization failed: {str(e)}", exc_info=True)
            raise
    return ydb_pool


async def set_user_state(user_id, mode):
    """Reliable implementation for setting user state in YDB"""
    try:
        logger.info(f"Setting user {user_id} state to {mode}")
        pool = await get_ydb_pool()
        
        def tx(session):
            # Method 1: Using new YDB parameter binding syntax
            try:
                query = f"""
                UPSERT INTO user_state (user_id, mode)
                VALUES ({user_id}, "{mode}")
                """
                session.transaction().execute(
                    query,
                    commit_tx=True
                )
                return
            except Exception as e:
                logger.warning(f"Method 1 failed: {str(e)}")
            
            # Method 2: Using old-style parameter binding
            try:
                query = """
                DECLARE $user_id AS Uint64;
                DECLARE $mode AS Utf8;
                
                UPSERT INTO user_state (user_id, mode) 
                VALUES ($user_id, $mode);
                """
                
                # Old-style parameter binding
                prepared_params = {
                    '$user_id': user_id,
                    '$mode': mode
                }
                
                session.transaction().execute(
                    query,
                    parameters=prepared_params,
                    commit_tx=True
                )
                return
            except Exception as e:
                logger.warning(f"Method 2 failed: {str(e)}")
            
            # Method 3: Using direct SQL with string formatting
            query = f"""
            UPSERT INTO user_state (user_id, mode)
            VALUES ({user_id}, '{mode.replace("'", "''")}')
            """
            session.transaction().execute(
                query,
                commit_tx=True
            )
        
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, pool.retry_operation_sync, tx)
        logger.info("User state saved successfully")
    except Exception as e:
        logger.error(f"Error setting user state: {str(e)}")
        raise

async def get_user_state(user_id):
    """Получает состояние пользователя из YDB"""
    try:
        logger.info(f"Getting state for user {user_id}")
        pool = await get_ydb_pool()
        
        def tx(session):
            # Method 1: Try new parameter binding style first
            try:
                query = f"""
                SELECT mode FROM user_state 
                WHERE user_id = {user_id};
                """
                result = session.transaction().execute(
                    query,
                    commit_tx=True
                )
                if result[0].rows:
                    return result[0].rows[0].mode
                return None
            except Exception as e:
                logger.warning(f"Method 1 failed: {str(e)}")
            
            # Method 2: Fallback to old parameter binding
            query = """
            DECLARE $user_id AS Uint64;
            
            SELECT mode FROM user_state 
            WHERE user_id = $user_id;
            """
            
            prepared_params = {
                '$user_id': user_id
            }
            
            result = session.transaction().execute(
                query,
                parameters=prepared_params,
                commit_tx=True
            )
            rows = result[0].rows
            return rows[0].mode if rows else None
        
        loop = asyncio.get_event_loop()
        state = await loop.run_in_executor(None, pool.retry_operation_sync, tx)
        logger.info(f"Retrieved user state: {state}")
        return state
    except Exception as e:
        logger.error(f"Error getting user state: {str(e)}")
        return None


def cleanup_temp_files():
    """Очищает временные файлы"""
    global _ydb_key_path
    if _ydb_key_path and os.path.exists(_ydb_key_path):
        try:
            os.unlink(_ydb_key_path)
            logger.info(f"Temporary YDB key file removed: {_ydb_key_path}")
        except Exception as e:
            logger.error(f"Error removing temporary file: {str(e)}")

async def init_user_state_table():
    """Создает таблицу user_state если она не существует"""
    try:
        logger.info("Initializing user_state table...")
        pool = await get_ydb_pool()
        
        def tx(session):
            session.transaction().execute(
                """
                CREATE TABLE IF NOT EXISTS user_state (
                    user_id Uint64,
                    mode Utf8,
                    PRIMARY KEY (user_id)
                )
                """,
                commit_tx=True
            )
        
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, pool.retry_operation_sync, tx)
        logger.info("user_state table initialized successfully")
    except Exception as e:
        if "already exists" in str(e).lower():
            logger.info("user_state table already exists")
        else:
            logger.error(f"Error creating user_state table: {str(e)}")
            raise

async def get_well_list_ydb(mode):
    """
    Получает список скважин из таблицы wells в YDB только за текущие сутки.
    """
    pool = await get_ydb_pool()
    today_str = date.today().strftime('%Y-%m-%d')

    def tx(session):
        query = f"""
        SELECT well_number FROM wells WHERE date = DATE('{today_str}')
        """
        result = session.transaction().execute(query, commit_tx=True)
        return [row.well_number for row in result[0].rows]

    loop = asyncio.get_event_loop()
    wells = await loop.run_in_executor(None, pool.retry_operation_sync, tx)
    return wells


async def get_well_description_ydb(well_number):
    """
    Получает описание скважины из YDB только за текущие сутки.
    """
    pool = await get_ydb_pool()
    today_str = date.today().strftime('%Y-%m-%d')

    def tx(session):
        # Экранируем кавычки в номере скважины на всякий случай
        safe_well_number = str(well_number).replace("'", "''")
        query = f"""
        SELECT description FROM wells
        WHERE well_number = '{safe_well_number}' AND date = DATE('{today_str}')
        """
        result = session.transaction().execute(query, commit_tx=True)
        rows = result[0].rows
        return rows[0].description if rows else "Скважина не найдена"

    loop = asyncio.get_event_loop()
    description = await loop.run_in_executor(None, pool.retry_operation_sync, tx)
    formatted = format_description(description)
    return formatted

def format_description(text: str) -> str:
    text = re.sub(r'(Работы за прошлые сутки[^\n\r:]*:)', r'🔷 <b>\1</b>', text)
    text = re.sub(r'(Работы за текущие сутки[^\n\r:]*:)', r'🔵 <b>\1</b>', text)
    text = re.sub(r'(Проблемные вопросы)', r'<b>\1</b>', text, flags=re.IGNORECASE)
    return text




# --- Google Sheets функции ---

# async def get_creds_from_object_storage():
#     """Загружает Google учетные данные из Object Storage"""
#     global _creds_dict
#     if _creds_dict is None:
#         try:
#             logger.info(f"Downloading Google credentials from {CREDS_URL}")
#             _creds_dict = await download_file(CREDS_URL, is_json=True)
#             logger.info("Google credentials downloaded successfully")
#         except Exception as e:
#             logger.error(f"Error downloading Google credentials: {str(e)}")
#             raise
    
#     creds = ServiceAccountCreds(
#         scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"],
#         **_creds_dict
#     )
#     return creds

# async def get_well_list(sheet_id, mode):
#     """Получает список скважин из Google Sheets"""
#     try:
#         logger.info(f"Getting well list for {mode} from sheet {sheet_id}")
#         creds = await get_creds_from_object_storage()
#         sheet_name = SHEET_NAMES[mode]
        
#         async with Aiogoogle(service_account_creds=creds) as aiogoogle:
#             sheets_api = await aiogoogle.discover("sheets", "v4")
#             result = await aiogoogle.as_service_account(
#                 sheets_api.spreadsheets.values.get(
#                     spreadsheetId=sheet_id,
#                     range=f"{sheet_name}!A2:A"
#                 )
#             )
#             wells = [row[0] for row in result.get("values", []) if row and row[0].strip()]
#             logger.info(f"Found {len(wells)} wells")
#             return wells
#     except Exception as e:
#         logger.error(f"Error getting well list: {str(e)}")
#         raise

# async def get_well_description(sheet_id, well_number, mode):
#     """Получает описание скважины из Google Sheets"""
#     try:
#         logger.info(f"Getting description for well {well_number} in mode {mode}")
#         creds = await get_creds_from_object_storage()
#         sheet_name = SHEET_NAMES[mode]
        
#         async with Aiogoogle(service_account_creds=creds) as aiogoogle:
#             sheets_api = await aiogoogle.discover("sheets", "v4")
#             result = await aiogoogle.as_service_account(
#                 sheets_api.spreadsheets.values.get(
#                     spreadsheetId=sheet_id,
#                     range=f"{sheet_name}!A2:B"
#                 )
#             )
#             for row in result.get("values", []):
#                 if row[0].strip() == well_number.strip():
#                     desc = row[1] if len(row) > 1 else "Описание работ не найдено"
#                     logger.info(f"Found description for well {well_number}")
#                     return desc
#             logger.info(f"Well {well_number} not found")
#             return "Скважина не найдена"
#     except Exception as e:
#         logger.error(f"Error getting well description: {str(e)}")
#         return "Ошибка при получении данных"

# --- YDB функции ---