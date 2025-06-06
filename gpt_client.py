import os
import logging
import asyncio
from yandex_cloud_ml_sdk import YCloudML
from yandex_cloud_ml_sdk.auth import APIKeyAuth
from dotenv import load_dotenv

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Загрузка переменных окружения
load_dotenv()

FOLDER_ID = os.getenv('FOLDER_ID')
API_KEY = os.getenv('YANDEX_API_KEY')  # Исправлено название переменной

def sync_get_summary(text: str) -> str | None:
    """Синхронная функция обращения к YandexGPT через ML SDK"""
    try:
        if not all([FOLDER_ID, API_KEY]):
            raise ValueError("Не заданы FOLDER_ID или YANDEX_API_KEY в окружении")
            
        sdk = YCloudML(
            folder_id=FOLDER_ID,
            auth=APIKeyAuth(API_KEY)  # Явное указание типа авторизации
        )
        
        model = sdk.models.completions("yandexgpt")
        prompt = (
            "Ты специалист по бурению нефтяных и газовых скважин. "
            "Суммируй текст кратко и по делу:\n"
            f"{text}"
        )
        
        result = model.run(prompt)
        return result[0].text.strip() if result else None
        
    except Exception as e:
        logger.error(f"Ошибка YandexGPT: {str(e)}")
        return None

async def get_summary(text: str) -> str | None:
    """Асинхронная обертка для синхронного вызова"""
    try:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            None, 
            lambda: sync_get_summary(text)  # Обернуто в lambda для перехвата исключений
        )
    except Exception as e:
        logger.error(f"Ошибка в асинхронном вызове: {str(e)}")
        return None
