import os
from yandex_cloud_ml_sdk import YCloudML
import asyncio

FOLDER_ID = os.getenv('FOLDER_ID')
API_KEY = os.getenv('YC_SA_ID')

def sync_get_summary(text: str) -> str | None:
    """Синхронная функция обращения к YandexGPT через ML SDK"""
    try:
        sdk = YCloudML(folder_id=FOLDER_ID, auth=API_KEY)
        model = sdk.models.completions("yandexgpt")
        prompt = (
            "Ты специалист по бурению нефтяных и газовых скважин. Суммируй текст кратко и по делу:\n"
            f"{text}"
        )
        result = model.run(prompt)
        return result[0].text.strip() if result else None
    except Exception as e:
        print(f"YandexGPT error: {e}")
        return None

async def get_summary(text: str) -> str | None:
    """Асинхронная обертка для синхронного вызова через run_in_executor"""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, sync_get_summary, text)
