import os
import logging
import asyncio
from yandex_cloud_ml_sdk import YCloudML
from yandex_cloud_ml_sdk.auth import APIKeyAuth

logger = logging.getLogger(__name__)

FOLDER_ID = os.getenv('FOLDER_ID')
YANDEX_API_KEY = os.getenv('YANDEX_API_KEY')

def sync_get_summary(text: str) -> str | None:
    try:
        if not all([FOLDER_ID, YANDEX_API_KEY]):
            logger.error("Не заданы FOLDER_ID или YANDEX_API_KEY в окружении")
            return None

        if not text or not text.strip():
            logger.error("Пустой текст для YandexGPT")
            return None

        logger.info(f"Запрос к YandexGPT. Длина текста: {len(text)} символов. Превью: {text[:100]!r}")

        sdk = YCloudML(
            folder_id=FOLDER_ID,
            auth=APIKeyAuth(YANDEX_API_KEY)
        )
        model = sdk.models.completions("yandexgpt",  model_version="rc")
        model = model.configure(temperature=0)
        prompt = (
            "Ты специалист по бурению нефтяных и газовых скважин. "
            "Суммируй текст кратко и по делу:\n"
            f"{text}"
        )

        logger.debug(f"Отправляемый prompt в YandexGPT: {prompt[:300]!r}")

        result = model.run(prompt)
        logger.info("Ответ от YandexGPT успешно получен")
        if result and hasattr(result[0], "text"):
            logger.debug(f"Ответ YandexGPT (первые 300 символов): {result[0].text[:300]!r}")
            return result[0].text.strip()
        else:
            logger.error("Пустой ответ от YandexGPT")
            return None

    except Exception as e:
        logger.error(f"Ошибка YandexGPT: {str(e)}", exc_info=True)
        return None

async def get_summary(text: str) -> str | None:
    """Асинхронная обертка для синхронного вызова"""
    try:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, sync_get_summary, text)
    except Exception as e:
        logger.error(f"Ошибка в асинхронном вызове YandexGPT: {str(e)}", exc_info=True)
        return None
