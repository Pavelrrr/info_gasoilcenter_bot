from dotenv import load_dotenv
import os
import json
import asyncio
import logging
from aiogram.types import Update
from bot import setup_bot, setup_dispatcher
from services import cleanup_temp_files

load_dotenv()

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def main_local():
    logger.info("Starting bot in polling mode...")
    try:
        await bot.delete_webhook(drop_pending_updates=True)
        await init_user_state_table()  # Добавьте эту строку
        await dp.start_polling(bot)
    except Exception as e:
        logger.error(f"Error in polling: {str(e)}")
    finally:
        cleanup_temp_files()
        
# Инициализация бота и диспетчера
bot = setup_bot()
dp = setup_dispatcher()

# Функция для обработки webhook-запросов
async def process_webhook_update(update_json):
    """Обрабатывает webhook-запрос от Telegram"""
    try:
        # Создаем объект Update из JSON
        update = Update(**update_json)
        await dp.feed_update(bot=bot, update=update)
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
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(process_webhook_update(update_json))
        finally:
            loop.close()
            
    except Exception as e:
        logger.error(f"Global error: {str(e)}", exc_info=True)
        return {"statusCode": 500, "body": json.dumps({"ok": False, "error": str(e)})}
    finally:
        # Очистка временных файлов
        cleanup_temp_files()

# Запуск бота в режиме поллинга (для локальной разработки)
if __name__ == "__main__":
    async def main_local():
        logger.info("Starting bot in polling mode...")
        try:
            # Удаляем webhook перед запуском polling
            await bot.delete_webhook(drop_pending_updates=True)
            await dp.start_polling(bot)
        except Exception as e:
            logger.error(f"Error in polling: {str(e)}")
        finally:
            cleanup_temp_files()
    
    asyncio.run(main_local())
