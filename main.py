import os
import json
import asyncio
import logging
from bot import setup_bot, setup_dispatcher
from services.ydb_storage import cleanup_temp_files

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Инициализация бота и диспетчера
bot = setup_bot()
dp = setup_dispatcher()

# Функция для обработки webhook-запросов
async def process_webhook_update(update_json):
    """Обрабатывает webhook-запрос от Telegram"""
    try:
        await dp.feed_update(bot=bot, update=update_json)
        return {"statusCode": 200, "body": json.dumps({"ok": True})}
    except Exception as e:
        logger.error(f"Error processing update: {str(e)}", exc_info=True)
        return {"statusCode": 500, "body": json.dumps({"ok": False, "error": str(e)})}

# Функция-обработчик для Yandex Cloud Functions
def handler(event, context):
    """Обработчик для Yandex Cloud Functions"""
    try:
        logger.info(f"Received event: {json.dumps(event)}")
        
        # Проверяем наличие тела запроса
        if 'body' not in event or not event['body']:
            logger.warning("Empty request body")
            return {"statusCode": 200, "body": json.dumps({"ok": True, "message": "No body provided"})}
        
        # Парсим JSON из тела запроса
        try:
            update_json = json.loads(event['body'])
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error: {str(e)}")
            return {"statusCode": 200, "body": json.dumps({"ok": True, "message": "Invalid JSON in body"})}
        
        # Обрабатываем обновление
        return asyncio.run(process_webhook_update(update_json))
    except Exception as e:
        logger.error(f"Global error: {str(e)}", exc_info=True)
        return {"statusCode": 500, "body": json.dumps({"ok": False, "error": str(e)})}
    finally:
        # Очистка временных файлов
        cleanup_temp_files()

# Запуск бота в режиме поллинга (для локальной разработки)
if __name__ == "__main__":
    async def on_startup():
        logger.info("Bot started")
    
    asyncio.run(dp.start_polling(bot, on_startup=on_startup))
