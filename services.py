import os
import logging
import asyncio
import tempfile
import ydb
from aiogoogle import Aiogoogle
from aiogoogle.auth.creds import ServiceAccountCreds
from utils import download_file

logger = logging.getLogger(__name__)

# Конфигурация Google Sheets
CREDS_URL = os.environ.get("CREDS_URL")
SHEET_NAMES = {
    "drilling": "08:00",
    "completion": "08:00 ОСВ"
}

# Конфигурация YDB
YDB_ENDPOINT = os.environ.get("YDB_ENDPOINT")
YDB_DATABASE = os.environ.get("YDB_DATABASE")
YDB_KEY_URL = os.environ.get("YDB_KEY_URL")

# Глобальные переменные
_creds_dict = None
_ydb_key_path = None
ydb_driver = None
ydb_pool = None

# --- Google Sheets функции ---

async def get_creds_from_object_storage():
    """Загружает Google учетные данные из Object Storage"""
    global _creds_dict
    if _creds_dict is None:
        try:
            logger.info(f"Downloading Google credentials from {CREDS_URL}")
            _creds_dict = await download_file(CREDS_URL, is_json=True)
            logger.info("Google credentials downloaded successfully")
        except Exception as e:
            logger.error(f"Error downloading Google credentials: {str(e)}")
            raise
    
    creds = ServiceAccountCreds(
        scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"],
        **_creds_dict
    )
    return creds

async def get_well_list(sheet_id, mode):
    """Получает список скважин из Google Sheets"""
    try:
        logger.info(f"Getting well list for {mode} from sheet {sheet_id}")
        creds = await get_creds_from_object_storage()
        sheet_name = SHEET_NAMES[mode]
        
        async with Aiogoogle(service_account_creds=creds) as aiogoogle:
            sheets_api = await aiogoogle.discover("sheets", "v4")
            result = await aiogoogle.as_service_account(
                sheets_api.spreadsheets.values.get(
                    spreadsheetId=sheet_id,
                    range=f"{sheet_name}!A2:A"
                )
            )
            wells = [row[0] for row in result.get("values", []) if row and row[0].strip()]
            logger.info(f"Found {len(wells)} wells")
            return wells
    except Exception as e:
        logger.error(f"Error getting well list: {str(e)}")
        raise

async def get_well_description(sheet_id, well_number, mode):
    """Получает описание скважины из Google Sheets"""
    try:
        logger.info(f"Getting description for well {well_number} in mode {mode}")
        creds = await get_creds_from_object_storage()
        sheet_name = SHEET_NAMES[mode]
        
        async with Aiogoogle(service_account_creds=creds) as aiogoogle:
            sheets_api = await aiogoogle.discover("sheets", "v4")
            result = await aiogoogle.as_service_account(
                sheets_api.spreadsheets.values.get(
                    spreadsheetId=sheet_id,
                    range=f"{sheet_name}!A2:B"
                )
            )
            for row in result.get("values", []):
                if row[0].strip() == well_number.strip():
                    desc = row[1] if len(row) > 1 else "Описание работ не найдено"
                    logger.info(f"Found description for well {well_number}")
                    return desc
            logger.info(f"Well {well_number} not found")
            return "Скважина не найдена"
    except Exception as e:
        logger.error(f"Error getting well description: {str(e)}")
        return "Ошибка при получении данных"

# --- YDB функции ---

async def get_ydb_key_path():
    """Загружает YDB ключ и сохраняет во временный файл"""
    global _ydb_key_path
    if _ydb_key_path is None:
        try:
            logger.info("Downloading YDB key")
            key_content = await download_file(YDB_KEY_URL)
            
            # Создаем временный файл для ключа
            temp_file = tempfile.NamedTemporaryFile(mode='w', delete=False)
            temp_file.write(key_content)
            temp_file.close()
            _ydb_key_path = temp_file.name
            
            logger.info(f"YDB key saved to temporary file: {_ydb_key_path}")
        except Exception as e:
            logger.error(f"Error getting YDB key: {str(e)}")
            raise
    return _ydb_key_path

def get_ydb_pool():
    """Инициализирует YDB драйвер и пул сессий"""
    global ydb_driver, ydb_pool
    if ydb_pool is None:
        try:
            # Получаем путь к ключу
            key_path = asyncio.run(get_ydb_key_path())
            
            # Проверяем, что файл существует и не пустой
            if not os.path.exists(key_path):
                raise FileNotFoundError(f"YDB key file not found at {key_path}")
                
            with open(key_path, 'r') as f:
                content = f.read()
                if not content:
                    raise ValueError("YDB key file is empty")
            
            # Инициализируем драйвер
            logger.info(f"Initializing YDB driver with endpoint: {YDB_ENDPOINT}, database: {YDB_DATABASE}")
            ydb_driver = ydb.Driver(
                endpoint=YDB_ENDPOINT,
                database=YDB_DATABASE,
                credentials=ydb.iam.ServiceAccountCredentials.from_file(key_path)
            )
            ydb_driver.wait(timeout=5)
            ydb_pool = ydb.SessionPool(ydb_driver)
            logger.info("YDB pool initialized successfully")
        except Exception as e:
            logger.error(f"YDB initialization failed: {str(e)}")
            raise
    return ydb_pool

def set_user_state(user_id, mode):
    """Сохраняет состояние пользователя в YDB"""
    try:
        logger.info(f"Setting user {user_id} state to {mode}")
        pool = get_ydb_pool()
        def tx(session):
            session.execute(
                "UPSERT INTO user_state (user_id, mode) VALUES (?, ?)",
                (user_id, mode)
            )
        pool.retry_operation_sync(tx)
        logger.info(f"User state saved successfully")
    except Exception as e:
        logger.error(f"Error setting user state: {str(e)}")
        raise

def get_user_state(user_id):
    """Получает состояние пользователя из YDB"""
    try:
        logger.info(f"Getting state for user {user_id}")
        pool = get_ydb_pool()
        def tx(session):
            result = session.transaction().execute(
                "SELECT mode FROM user_state WHERE user_id = ?",
                (user_id,),
                commit_tx=True
            )
            rows = result[0].rows
            return rows[0].mode if rows else None
        state = pool.retry_operation_sync(tx)
        logger.info(f"User state: {state}")
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
