import os
import json
import httpx
import asyncio
import ydb
from aiogoogle import Aiogoogle
from aiogoogle.auth.creds import ServiceAccountCreds
import logging  # LOG: Добавлен модуль логирования

# Настройка логирования
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# --- Конфигурация ---
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
CREDS_URL = os.environ.get("CREDS_URL")
SHEET_IDS = {
    "drilling": os.environ.get("DRILLING_SHEET_ID"),
    "completion": os.environ.get("COMPLETION_SHEET_ID")
}
SHEET_NAMES = {
    "drilling": "08:00",
    "completion": "08:00 ОСВ"
}

# --- YDB параметры ---
YDB_ENDPOINT = os.environ.get("YDB_ENDPOINT")
YDB_DATABASE = os.environ.get("YDB_DATABASE")
YDB_KEY_PATH = os.environ.get("YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS")

# --- Кэш для creds.json ---
_creds_dict = None

# --- YDB pool ---
ydb_driver = None
ydb_pool = None

def get_ydb_pool():
    global ydb_driver, ydb_pool
    if ydb_pool is None:
        try:  # LOG: Добавлена обработка ошибок инициализации
            logger.info("Initializing YDB driver...")
            ydb_driver = ydb.Driver(
                endpoint=YDB_ENDPOINT,
                database=YDB_DATABASE,
                credentials=ydb.iam.ServiceAccountCredentials.from_file(YDB_KEY_PATH)
            )
            ydb_driver.wait(timeout=5)
            ydb_pool = ydb.SessionPool(ydb_driver)
            logger.info("YDB pool initialized successfully")
        except Exception as e:
            logger.error(f"YDB initialization failed: {str(e)}")
            raise
    return ydb_pool

def set_user_state(user_id, mode):
    try:  # LOG: Добавлено логирование операций с YDB
        logger.info(f"Setting user {user_id} state to {mode}")
        pool = get_ydb_pool()
        def tx(session):
            session.execute(
                "UPSERT INTO user_state (user_id, mode) VALUES (?, ?)",
                (user_id, mode)
            )
        pool.retry_operation_sync(tx)
    except Exception as e:
        logger.error(f"Error setting user state: {str(e)}")
        raise

def get_user_state(user_id):
    try:  # LOG: Логирование операций чтения
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
        return pool.retry_operation_sync(tx)
    except Exception as e:
        logger.error(f"Error getting user state: {str(e)}")
        return None

async def get_creds_from_object_storage():
    global _creds_dict
    if _creds_dict is None:
        try:  # LOG: Логирование загрузки creds
            logger.info("Downloading creds from object storage")
            async with httpx.AsyncClient() as client:
                response = await client.get(CREDS_URL)
                response.raise_for_status()
                _creds_dict = response.json()
            logger.info("Creds downloaded successfully")
        except Exception as e:
            logger.error(f"Error downloading creds: {str(e)}")
            raise
    return ServiceAccountCreds(
        scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"],
        **_creds_dict
    )

async def get_well_list(sheet_id, mode):
    try:  # LOG: Логирование работы с Google Sheets
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
            logger.info(f"Found {len(wells)} wells for {mode}")
            return wells
    except Exception as e:
        logger.error(f"Error getting well list: {str(e)}")
        raise

async def get_well_description(sheet_id, well_number, mode):
    try:  # LOG: Логирование поиска описания
        logger.info(f"Getting description for {well_number} in {mode}")
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
                    return row[1] if len(row) > 1 else "Описание работ не найдено"
            return "Скважина не найдена"
    except Exception as e:
        logger.error(f"Error getting well description: {str(e)}")
        return "Ошибка при получении данных"

async def send_message(chat_id, text, reply_markup=None):
    try:  # LOG: Логирование отправки сообщений
        logger.info(f"Sending message to chat {chat_id}")
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML"}
        if reply_markup:
            payload["reply_markup"] = json.dumps(reply_markup)
        
        async with httpx.AsyncClient() as client:
            response = await client.post(url, json=payload)
            response.raise_for_status()
            logger.info(f"Message sent successfully to {chat_id}")
            return response.json()
    except Exception as e:
        logger.error(f"Error sending message: {str(e)}")
        raise

def make_keyboard(buttons, row_width=3):
    try:  # LOG: Логирование создания клавиатуры
        logger.info(f"Creating keyboard with {len(buttons)} buttons")
        keyboard = []
        for i in range(0, len(buttons), row_width):
            keyboard.append([{"text": btn, "callback_data": btn} for btn in buttons[i:i+row_width]])
        return {"inline_keyboard": keyboard}
    except Exception as e:
        logger.error(f"Error creating keyboard: {str(e)}")
        return {"inline_keyboard": []}

def handler(event, context):
    logger.info("Received event: %s", json.dumps(event))  # LOG: Логирование входящего события
    
    try:
        if 'body' not in event or not event['body']:
            logger.warning("Empty request body received")
            return {"statusCode": 200, "body": json.dumps({"ok": True, "message": "No body provided"})}
        
        try:
            data = json.loads(event['body'])
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error: {str(e)}")
            return {"statusCode": 200, "body": json.dumps({"ok": True, "message": "Invalid JSON in body"})}
        
        logger.debug("Parsed data: %s", json.dumps(data))
        loop = asyncio.get_event_loop()

        if "message" in data:
            msg = data["message"]
            chat_id = msg["chat"]["id"]
            user_id = msg["from"]["id"]
            text = msg.get("text", "")
            logger.info(f"Received message from {user_id}: {text}")

            if text == "/start":
                logger.info("Processing /start command")
                keyboard = {
                    "inline_keyboard": [
                        [{"text": "Бурение", "callback_data": "drilling"}],
                        [{"text": "Освоение", "callback_data": "completion"}]
                    ]
                }
                loop.run_until_complete(send_message(chat_id, "Выберите режим:", keyboard))
                return {"statusCode": 200, "body": json.dumps({"ok": True})}

        if "callback_query" in data:
            callback = data["callback_query"]
            chat_id = callback["message"]["chat"]["id"]
            user_id = callback["from"]["id"]
            data_str = callback["data"]
            logger.info(f"Processing callback: {data_str} from {user_id}")

            if data_str in ("drilling", "completion"):
                logger.info(f"User {user_id} selected mode: {data_str}")
                try:
                    set_user_state(user_id, data_str)
                    wells = loop.run_until_complete(get_well_list(SHEET_IDS[data_str], data_str))
                    keyboard = make_keyboard(wells, row_width=3)
                    loop.run_until_complete(send_message(chat_id, "Выберите скважину:", keyboard))
                except Exception as e:
                    logger.error(f"Error processing mode selection: {str(e)}")
                    loop.run_until_complete(send_message(chat_id, "⚠️ Ошибка при загрузке данных"))
                return {"statusCode": 200, "body": json.dumps({"ok": True})}

            mode = get_user_state(user_id)
            if mode and data_str:
                logger.info(f"Processing well selection: {data_str} in mode {mode}")
                try:
                    description = loop.run_until_complete(get_well_description(SHEET_IDS[mode], data_str, mode))
                    loop.run_until_complete(send_message(chat_id, f"<b>{data_str}</b>\n{description}"))
                except Exception as e:
                    logger.error(f"Error processing well request: {str(e)}")
                    loop.run_until_complete(send_message(chat_id, "⚠️ Ошибка при получении описания"))
                return {"statusCode": 200, "body": json.dumps({"ok": True})}

        logger.warning("Unhandled request type")
        return {"statusCode": 200, "body": json.dumps({"ok": True})}

    except Exception as e:  # LOG: Глобальный обработчик ошибок
        logger.error(f"Global error: {str(e)}", exc_info=True)
        return {"statusCode": 500, "body": json.dumps({"ok": False, "error": str(e)})}
