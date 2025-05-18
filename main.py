import os
import json
import httpx
import asyncio
import ydb
from aiogoogle import Aiogoogle
from aiogoogle.auth.creds import ServiceAccountCreds

# --- Конфигурация ---
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
CREDS_URL = os.environ.get("CREDS_URL")  # Публичная ссылка на creds.json из Object Storage
SHEET_IDS = {
    "drilling": os.environ.get("DRILLING_SHEET_ID"),
    "completion": os.environ.get("COMPLETION_SHEET_ID")
}
SHEET_NAMES = {
    "drilling": "08:00",
    "completion": "08:00 ОСВ"  # Изменено название вкладки для режима освоения
}

# --- YDB параметры ---
YDB_ENDPOINT = os.environ.get("YDB_ENDPOINT")
YDB_DATABASE = os.environ.get("YDB_DATABASE")
YDB_KEY_PATH = os.environ.get("YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS")

# --- Кэш для creds.json ---
_creds_dict = None

# --- YDB pool (глобально, чтобы не пересоздавать) ---
ydb_driver = None
ydb_pool = None

def get_ydb_pool():
    global ydb_driver, ydb_pool
    if ydb_pool is None:
        ydb_driver = ydb.Driver(
            endpoint=YDB_ENDPOINT,
            database=YDB_DATABASE,
            credentials=ydb.iam.ServiceAccountCredentials.from_file(YDB_KEY_PATH)
        )
        ydb_driver.wait(timeout=5)
        ydb_pool = ydb.SessionPool(ydb_driver)
    return ydb_pool

def set_user_state(user_id, mode):
    pool = get_ydb_pool()
    def tx(session):
        session.execute(
            "UPSERT INTO user_state (user_id, mode) VALUES (?, ?)",
            (user_id, mode)
        )
    pool.retry_operation_sync(tx)

def get_user_state(user_id):
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

async def get_creds_from_object_storage():
    global _creds_dict
    if _creds_dict is None:
        async with httpx.AsyncClient() as client:
            response = await client.get(CREDS_URL)
            response.raise_for_status()
            _creds_dict = response.json()
    creds = ServiceAccountCreds(
        scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"],
        **_creds_dict
    )
    return creds

async def get_well_list(sheet_id, mode):
    creds = await get_creds_from_object_storage()
    sheet_name = SHEET_NAMES[mode]  # Используем соответствующее название вкладки
    async with Aiogoogle(service_account_creds=creds) as aiogoogle:
        sheets_api = await aiogoogle.discover("sheets", "v4")
        result = await aiogoogle.as_service_account(
            sheets_api.spreadsheets.values.get(
                spreadsheetId=sheet_id,
                range=f"{sheet_name}!A2:A"
            )
        )
        wells = [row[0] for row in result.get("values", []) if row and row[0].strip()]
        return wells

async def get_well_description(sheet_id, well_number, mode):
    creds = await get_creds_from_object_storage()
    sheet_name = SHEET_NAMES[mode]  # Используем соответствующее название вкладки
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

async def send_message(chat_id, text, reply_markup=None):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML"}
    if reply_markup:
        payload["reply_markup"] = json.dumps(reply_markup)
    async with httpx.AsyncClient() as client:
        await client.post(url, json=payload)

def make_keyboard(buttons, row_width=3):
    # Формирует inline клавиатуру из списка кнопок
    keyboard = []
    for i in range(0, len(buttons), row_width):
        keyboard.append([{"text": btn, "callback_data": btn} for btn in buttons[i:i+row_width]])
    return {"inline_keyboard": keyboard}

def handler(event, context):
    # Проверка наличия body и его обработка
    if 'body' not in event or not event['body']:
        return {"statusCode": 200, "body": json.dumps({"ok": True, "message": "No body provided"})}
    
    try:
        data = json.loads(event['body'])
    except json.JSONDecodeError:
        # Возможно, это тестовый вызов или прямой GET-запрос
        return {"statusCode": 200, "body": json.dumps({"ok": True, "message": "Invalid JSON in body"})}
    
    loop = asyncio.get_event_loop()

    if "message" in data:
        chat_id = data["message"]["chat"]["id"]
        user_id = data["message"]["from"]["id"]
        text = data["message"]["text"]

        # Главное меню
        if text == "/start":
            keyboard = {
                "inline_keyboard": [
                    [{"text": "Бурение", "callback_data": "drilling"}],
                    [{"text": "Освоение", "callback_data": "completion"}]
                ]
            }
            loop.run_until_complete(send_message(chat_id, "Выберите режим:", keyboard))
            return {"statusCode": 200, "body": json.dumps({"ok": True})}

    # Обработка нажатий на кнопки (callback_query)
    if "callback_query" in data:
        callback = data["callback_query"]
        chat_id = callback["message"]["chat"]["id"]
        user_id = callback["from"]["id"]
        data_str = callback["data"]

        # Если выбрали режим
        if data_str in ("drilling", "completion"):
            set_user_state(user_id, data_str)
            wells = loop.run_until_complete(get_well_list(SHEET_IDS[data_str], data_str))
            keyboard = make_keyboard(wells, row_width=3)
            loop.run_until_complete(send_message(chat_id, "Выберите скважину:", keyboard))
            return {"statusCode": 200, "body": json.dumps({"ok": True})}

        # Если выбрали скважину
        mode = get_user_state(user_id)
        if mode and data_str:
            description = loop.run_until_complete(get_well_description(SHEET_IDS[mode], data_str, mode))
            loop.run_until_complete(send_message(chat_id, f"<b>{data_str}</b>\n{description}"))
            return {"statusCode": 200, "body": json.dumps({"ok": True})}

    # Ответ по умолчанию
    return {"statusCode": 200, "body": json.dumps({"ok": True})}
