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

async def process_webhook_update(update_json):
    """Обрабатывает webhook-запрос от Telegram"""
    bot = None
    try:
        bot = setup_bot()
        dp = setup_dispatcher()
        
        # Создаем объект Update из JSON
        update = Update(**update_json)
        
        # Обрабатываем обновление
        await dp.feed_update(bot=bot, update=update)
        
        return {"statusCode": 200, "body": json.dumps({"ok": True})}
    except Exception as e:
        logger.error(f"Error processing update: {str(e)}", exc_info=True)
        return {"statusCode": 500, "body": json.dumps({"ok": False, "error": str(e)})}
    finally:
        # Закрываем HTTP сессию бота
        if bot:
            try:
                await bot.session.close()
            except Exception as e:
                logger.warning(f"Error closing bot session: {e}")

def handler(event, context):
    """Упрощенный обработчик для Yandex Cloud Functions"""
    try:
        logger.info(f"Received event")
        
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
        
        # Используем asyncio.run() для простоты
        result = asyncio.run(process_webhook_update(update_json))
        return result
        
    except Exception as e:
        logger.error(f"Global error: {str(e)}", exc_info=True)
        return {"statusCode": 500, "body": json.dumps({"ok": False, "error": str(e)})}
    finally:
        # Очистка временных файлов
        try:
            cleanup_temp_files()
        except Exception as e:
            logger.warning(f"Error cleaning up temp files: {e}")

# Для локального тестирования
if __name__ == "__main__":
    async def main_local():
        logger.info("Starting bot in polling mode...")
        try:
            bot = setup_bot()
            dp = setup_dispatcher()
            await bot.delete_webhook(drop_pending_updates=True)
            await dp.start_polling(bot)
        except Exception as e:
            logger.error(f"Error in polling: {str(e)}")
        finally:
            cleanup_temp_files()
    
    asyncio.run(main_local())
