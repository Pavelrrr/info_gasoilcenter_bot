import os
import logging
from aiogram import Bot, Dispatcher
from aiogram.enums import ParseMode
from aiogram.types import Message, CallbackQuery
from aiogram.filters import Command
from aiogram.utils.keyboard import InlineKeyboardBuilder
from services import set_user_state, get_user_state
from services import get_well_list, get_well_description
from aiogram.client.default import DefaultBotProperties
from dotenv import load_dotenv

load_dotenv() 

# Конфигурация
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
logger = logging.getLogger(__name__)

# Получаем ID таблиц из переменных окружения
SHEET_IDS = {
    "drilling": os.environ.get("DRILLING_SHEET_ID"),
    "completion": os.environ.get("COMPLETION_SHEET_ID")
}

def setup_bot():
    return Bot(
        token=TELEGRAM_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML)
    )

def setup_dispatcher():
    """Создает и настраивает диспетчер"""
    dp = Dispatcher()
    
    # Регистрируем все обработчики
    register_all_handlers(dp)
    
    return dp

def register_all_handlers(dp: Dispatcher):
    """Регистрирует все обработчики"""
    register_start_handlers(dp)
    register_wells_handlers(dp)

async def cmd_start(message: Message):
    """Обработчик команды /start"""
    try:
        logger.info(f"Processing /start command from user {message.from_user.id}")
        
        # Отправляем сообщение с клавиатурой выбора режима
        await message.answer("Выберите режим:", reply_markup=get_mode_keyboard())
    except Exception as e:
        logger.error(f"Error in start command: {str(e)}")
        await message.answer("⚠️ Произошла ошибка при обработке команды")

def register_start_handlers(dp: Dispatcher):
    """Регистрирует обработчики команды старт"""
    dp.message.register(cmd_start, Command("start"))

async def process_mode_selection(callback: CallbackQuery):
    """Обработчик выбора режима"""
    try:
        user_id = callback.from_user.id
        mode = callback.data
        logger.info(f"User {user_id} selected mode: {mode}")
        
        # Проверим, что mode не None и не пустая строка
        if not mode:
            logger.error(f"Mode is empty or None: {mode}")
            await callback.message.answer("⚠️ Ошибка: не выбран режим")
            await callback.answer()
            return
        
        # Сохраняем выбранный режим
        await set_user_state(user_id, mode)
        
        # Получаем список скважин
        wells = await get_well_list(SHEET_IDS[mode], mode)
        
        # Отправляем сообщение с клавиатурой выбора скважины
        await callback.message.answer(
            "Выберите скважину:", 
            reply_markup=get_wells_keyboard(wells)
        )
        await callback.answer()
    except Exception as e:
        logger.error(f"Error processing mode selection: {str(e)}")
        await callback.message.answer("⚠️ Ошибка при загрузке данных")
        await callback.answer()


async def process_well_selection(callback: CallbackQuery):
    """Обработчик выбора скважины"""
    try:
        user_id = callback.from_user.id
        well_number = callback.data
        
        # Получаем режим пользователя (добавлен await)
        mode = await get_user_state(user_id)
        
        if mode:
            logger.info(f"Processing well selection {well_number} in mode {mode}")
            
            # Получаем описание скважины
            description = await get_well_description(SHEET_IDS[mode], well_number, mode)
            
            # Отправляем сообщение
            await callback.message.answer(f"<b>{well_number}</b>\n{description}")
            await callback.answer()
        else:
            logger.warning(f"User {user_id} has no mode selected")
            await callback.message.answer("⚠️ Сначала выберите режим")
            await callback.answer()
    except Exception as e:
        logger.error(f"Error processing well selection: {str(e)}")
        await callback.message.answer("⚠️ Ошибка при получении описания")
        await callback.answer()

def register_wells_handlers(dp: Dispatcher):
    """Регистрирует обработчики выбора режима и скважин"""
    # Обработчик выбора режима
    dp.callback_query.register(
        process_mode_selection,
        lambda c: c.data in ["drilling", "completion"]
    )
    
    # Обработчик выбора скважины (все остальные колбэки)
    dp.callback_query.register(process_well_selection)

def get_mode_keyboard():
    """Создает клавиатуру выбора режима"""
    builder = InlineKeyboardBuilder()
    builder.button(text="Бурение", callback_data="drilling")
    builder.button(text="Освоение", callback_data="completion")
    return builder.as_markup()

def get_wells_keyboard(wells, row_width=3):
    """Создает клавиатуру выбора скважины"""
    builder = InlineKeyboardBuilder()
    for well in wells:
        builder.button(text=well, callback_data=well)
    builder.adjust(row_width)  # Кнопок в ряду
    return builder.as_markup()
