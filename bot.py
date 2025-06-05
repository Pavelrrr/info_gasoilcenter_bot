import os
import logging
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher
from aiogram.enums import ParseMode
from aiogram.types import Message, CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup
from aiogram.filters import Command
from aiogram.utils.keyboard import InlineKeyboardBuilder
from services import set_user_state, get_user_state
from services import get_well_list_ydb, get_well_description_ydb
from aiogram.client.default import DefaultBotProperties

MAX_MESSAGE_LENGTH = 4096
load_dotenv()
def split_message(text, max_length=MAX_MESSAGE_LENGTH):
    # Разбивает текст на части по границе строки или пробела, чтобы не резать слова
    parts = []
    while text:
        if len(text) <= max_length:
            parts.append(text)
            break
        part = text[:max_length]
        last_n = part.rfind('\n')
        last_sp = part.rfind(' ')
        split_at = max(last_n, last_sp)
        if split_at == -1:
            split_at = max_length
        parts.append(text[:split_at])
        text = text[split_at:].lstrip()
    return parts



# Конфигурация
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
logger = logging.getLogger(__name__)

# # Получаем ID таблиц из переменных окружения
# SHEET_IDS = {
#     "drilling": os.environ.get("DRILLING_SHEET_ID"),
#     "completion": os.environ.get("COMPLETION_SHEET_ID")
# }

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

def register_wells_handlers(dp: Dispatcher):
    """Регистрирует обработчики выбора режима и скважин"""
    # Обработчик кнопки "Начать"
    dp.callback_query.register(
        process_start_button,
        lambda c: c.data == "start_bot"
    )
    
    # Обработчик выбора режима
    dp.callback_query.register(
        process_mode_selection,
        lambda c: c.data in ["drilling", "completion"]
    )
    
    # Обработчик возврата к списку скважин
    dp.callback_query.register(
        process_back_to_wells,
        lambda c: c.data == "back_to_wells"
    )
    
    # Обработчик выбора скважины (все остальные колбэки)
    dp.callback_query.register(process_well_selection)

async def cmd_start(message: Message):
    """Обработчик команды /start"""
    try:
        logger.info(f"Processing /start command from user {message.from_user.id}")
        
        # Показываем приветствие с кнопкой "Начать"
        builder = InlineKeyboardBuilder()
        builder.button(text="🚀 Начать", callback_data="start_bot")
        
        await message.answer(
            "🔧 Добро пожаловать в бот для работы со скважинами!\n\n"
            "Нажмите кнопку ниже для начала работы:",
            reply_markup=builder.as_markup()
        )
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
        
        if not mode:
            logger.error(f"Mode is empty or None: {mode}")
            await callback.message.answer("⚠️ Ошибка: не выбран режим")
            await callback.answer()
            return
        
        # Сохраняем выбранный режим
        await set_user_state(user_id, mode)
        
        # Получаем список скважин
        wells = await get_well_list_ydb(mode)
        
        # Определяем название режима для отображения
        mode_text = "Бурение" if mode == "drilling" else "Освоение"
        
        # Отправляем сообщение с клавиатурой выбора скважины
        await callback.message.edit_text(
            f"🔧 <b>Режим: {mode_text}</b>\n\n"
            "Выберите скважину:",
            reply_markup=get_wells_keyboard(wells)
        )
        await callback.answer()
    except Exception as e:
        logger.error(f"Error processing mode selection: {str(e)}")
        await callback.message.edit_text("⚠️ Ошибка при загрузке данных")
        await callback.answer()



async def process_well_selection(callback: CallbackQuery):
    """Обработчик выбора скважины"""
    try:
        user_id = callback.from_user.id
        well_number = callback.data

        # Проверяем специальные команды навигации
        if well_number == "back_to_modes":
            await callback.message.edit_text(
                "Выберите режим работы:",
                reply_markup=get_mode_keyboard()
            )
            await callback.answer()
            return

        if well_number == "back_to_start":
            builder = InlineKeyboardBuilder()
            builder.button(text="🚀 Начать", callback_data="start_bot")
            await callback.message.edit_text(
                "🔧 Добро пожаловать в бот для работы со скважинами!\n\n"
                "Нажмите кнопку ниже для начала работы:",
                reply_markup=builder.as_markup()
            )
            await callback.answer()
            return

        # Получаем режим пользователя
        mode = await get_user_state(user_id)

        if mode:
            logger.info(f"Processing well selection {well_number} in mode {mode}")

            # Получаем описание скважины
            description = await get_well_description_ydb(well_number)

            # Создаем клавиатуру с кнопками возврата
            builder = InlineKeyboardBuilder()
            builder.row(
                InlineKeyboardButton(text="🔙 К списку скважин", callback_data="back_to_wells"),
                InlineKeyboardButton(text="🔄 К выбору режима", callback_data="back_to_modes")
            )
            builder.row(
                InlineKeyboardButton(text="🏠 В начало", callback_data="back_to_start")
            )

            full_text = f"🔹 Скважина {well_number}\n\n📋 Описание работ:\n{description}"
            parts = split_message(full_text)

            # Удаляем старое сообщение
            await callback.message.delete()

            # Отправляем все части, кроме последней, без клавиатуры
            for part in parts[:-1]:
                await callback.message.answer(part)

            # Последнюю часть отправляем с клавиатурой
            await callback.message.answer(parts[-1], reply_markup=builder.as_markup())

            await callback.answer()
        else:
            await callback.answer("Режим не выбран.")
    except Exception as e:
        logger.error(f"Error processing well selection: {str(e)}")
        await callback.answer("⚠️ Ошибка при получении описания")


def get_mode_keyboard():
    """Создает клавиатуру выбора режима"""
    builder = InlineKeyboardBuilder()
    builder.button(text="🔧 Бурение", callback_data="drilling")
    builder.button(text="🛠 Освоение", callback_data="completion")
    builder.adjust(2)  # Два режима в одном ряду
    
    # Добавляем кнопку возврата
    builder.row(
        InlineKeyboardButton(text="🏠 В начало", callback_data="back_to_start")
    )
    
    return builder.as_markup()

def get_wells_keyboard(wells, row_width=3):
    """Создает клавиатуру выбора скважины с кнопкой возврата"""
    builder = InlineKeyboardBuilder()
    
    # Добавляем кнопки скважин
    for well in wells:
        builder.button(text=well, callback_data=well)
    
    # Настраиваем расположение кнопок скважин
    builder.adjust(row_width)
    
    # Добавляем кнопки навигации в отдельный ряд
    builder.row(
        InlineKeyboardButton(text="🔙 К выбору режима", callback_data="back_to_modes"),
        InlineKeyboardButton(text="🏠 В начало", callback_data="back_to_start")
    )
    
    return builder.as_markup()

async def process_start_button(callback: CallbackQuery):
    """Обработчик кнопки 'Начать'"""
    try:
        logger.info(f"User {callback.from_user.id} pressed start button")
        
        # Показываем меню выбора режима
        await callback.message.edit_text(
            "Выберите режим работы:",
            reply_markup=get_mode_keyboard()
        )
        await callback.answer()
    except Exception as e:
        logger.error(f"Error processing start button: {str(e)}")
        await callback.answer("⚠️ Произошла ошибка")

async def process_back_to_wells(callback: CallbackQuery):
    """Обработчик возврата к списку скважин"""
    try:
        user_id = callback.from_user.id
        mode = await get_user_state(user_id)
        
        if mode:
            # Получаем список скважин заново
            wells = await get_well_list_ydb(mode)
            
            # Показываем список скважин
            mode_text = "Бурение" if mode == "drilling" else "Освоение"
            await callback.message.edit_text(
                f"🔧 <b>Режим: {mode_text}</b>\n\n"
                "Выберите скважину:",
                reply_markup=get_wells_keyboard(wells)
            )
            await callback.answer()
        else:
            await callback.message.edit_text(
                "Выберите режим работы:",
                reply_markup=get_mode_keyboard()
            )
            await callback.answer()
    except Exception as e:
        logger.error(f"Error returning to wells list: {str(e)}")
        await callback.answer("⚠️ Произошла ошибка")

def split_message(text, max_length=4000):
    """Разбивает длинное сообщение на части"""
    if len(text) <= max_length:
        return [text]
    
    messages = []
    while len(text) > max_length:
        # Ищем последний пробел в пределах лимита
        split_pos = text.rfind(' ', 0, max_length)
        if split_pos == -1:  # Если нет пробелов, режем по лимиту
            split_pos = max_length
        
        messages.append(text[:split_pos])
        text = text[split_pos:].lstrip()
    
    if text:  # Добавляем остаток
        messages.append(text)
    
    return messages