import os
import logging
import asyncio
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher
from aiogram.enums import ParseMode
from aiogram.types import Message, CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup
from aiogram.filters import Command
from aiogram.utils.keyboard import InlineKeyboardBuilder
from services import set_user_state, get_user_state
from services import get_well_list_ydb, get_well_description_ydb, get_ydb_pool
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
    try:
        user_id = callback.from_user.id
        well_number = callback.data

        # ... ваши проверки back_to_modes и back_to_start ...

        mode = await get_user_state(user_id)

        if mode:
            logger.info(f"Processing well selection {well_number} in mode {mode}")

            # 1. Удаляем старую клавиатуру (если message_id сохранён)
            last_msg_id = await get_user_message_id(user_id)
            if last_msg_id:
                try:
                    await callback.bot.edit_message_reply_markup(
                        chat_id=callback.message.chat.id,
                        message_id=last_msg_id,
                        reply_markup=None
                    )
                except Exception as e:
                    logger.warning(f"Не удалось удалить старую клавиатуру: {e}")

            # 2. Получаем описание скважины
            description = await get_well_description_ydb(well_number)

            # 3. Отправляем описание (без клавиатуры)
            full_text = f"🔹 <b>Скважина {well_number}</b>\n\n📋 Описание работ:\n{description}"
            parts = split_message(full_text)
            for part in parts:
                await callback.message.answer(part, parse_mode="HTML")

            # 4. Отправляем новое сообщение с клавиатурой
            builder = InlineKeyboardBuilder()
            builder.row(
                InlineKeyboardButton(text="🔙 К списку скважин", callback_data="back_to_wells"),
                InlineKeyboardButton(text="🔄 К выбору режима", callback_data="back_to_modes")
            )
            builder.row(
                InlineKeyboardButton(text="🏠 В начало", callback_data="back_to_start")
            )
            keyboard_msg = await callback.message.answer(
                "Выберите действие:",
                reply_markup=builder.as_markup()
            )

            # 5. Сохраняем новый message_id в user_state
            await set_user_message_id(user_id, keyboard_msg.message_id)

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

async def set_user_message_id(user_id: int, message_id: int):
    pool = await get_ydb_pool()
    def tx(session):
        query = """
        UPSERT INTO user_states (user_id, message_id)
        VALUES ($user_id, $message_id)
        """
        params = {
            "$user_id": user_id,
            "$message_id": message_id
        }
        session.transaction().execute(
            query,
            parameters=params,
            commit_tx=True
        )
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, pool.retry_operation_sync, tx)

async def get_user_message_id(user_id: int):
    pool = await get_ydb_pool()
    def tx(session):
        query = """
        SELECT message_id FROM user_states WHERE user_id = $user_id
        """
        params = {"$user_id": user_id}
        result = session.transaction().execute(
            query,
            parameters=params,
            commit_tx=True
        )
        return result[0].rows[0].message_id if result[0].rows else None
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, pool.retry_operation_sync, tx)


