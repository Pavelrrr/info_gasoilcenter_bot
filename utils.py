import logging
import httpx

logger = logging.getLogger(__name__)

async def download_file(url, is_json=False):
    """Загружает файл по URL"""
    try:
        logger.info(f"Downloading file from {url}")
        async with httpx.AsyncClient() as client:
            response = await client.get(url)
            response.raise_for_status()
            if is_json:
                return response.json()
            else:
                return response.text
    except Exception as e:
        logger.error(f"Error downloading from {url}: {str(e)}")
        raise
