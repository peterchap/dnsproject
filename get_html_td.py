import aiosqlite
import asyncio
import httpx
import polars as pl
import time
from aiolimiter import AsyncLimiter
from datetime import datetime
from loguru import logger as custom_logger
from sys import stdout

logdir = "/root/"
LOGGER = custom_logger


def formatter(log: dict) -> str:
    """
    Format log colors based on level.
    :param dict log: Dictionary containing log level, details, etc.
    :returns: str
    """
    if log["level"].name == "INFO":
        return (
            "<fg #aad1f7>{time:HH:mm:ss A}</fg #aad1f7> | "
            "<fg #cfe2f3>{level}</fg #cfe2f3>: "
            "<light-white>{message}</light-white> \n"
        )
    if log["level"].name == "SUCCESS":
        return (
            "<fg #aad1f7>{time:HH:mm:ss A}</fg #aad1f7> | "
            "<fg #85de83>{level}</fg #85de83>: "
            "<light-white>{message}</light-white> \n"
        )
    if log["level"].name == "WARNING":
        return (
            "<fg #aad1f7>{time:HH:mm:ss A}</fg #aad1f7> | "
            "<light-yellow>{level}</light-yellow>: "
            "<light-white>{message}</light-white> \n"
        )
    elif log["level"].name == "ERROR":
        return (
            "<fg #aad1f7>{time:HH:mm:ss A}</fg #aad1f7> | "
            "<light-red>{level}</light-red>: "
            "<light-white>{message}</light-white> \n"
        )
    else:
        return (
            "<fg #aad1f7>{time:HH:mm:ss A}</fg #aad1f7> | "
            "<fg #67c9c4>{level}</fg #67c9c4>: "
            "<light-white>{message}</light-white> \n"
        )


def create_logger():
    """
    Create custom logger.
    :returns: custom_logger
    """
    custom_logger.remove()
    custom_logger.add(stdout, colorize=True, format=formatter)
    custom_logger.add(logdir + "dnslog.log", encoding="utf-8")
    return custom_logger


async def fetch_url(session, url):
    try:
        html = await session.get(url)
        LOGGER.info(f"url: {url} - status: {html.status_code}")
        return html.status_code, html.text
    except httpx.ConnectError as e:
        LOGGER.error(f"Connection error for {e.request.url!r}.")
        return 102, f"Connection error for {e.request.url!r} - {e}"
    except httpx.ConnectTimeout as e:
        LOGGER.error(f"Connection timeout for {e.request.url!r}.")
        return 103, f"Connection timeout for {e.request.url!r} - {e}"
    except httpx.RequestError as e:
        LOGGER.error(f"Request error block for {e.request.url!r}.")
        return (
            104,
            f"Request error block {e.request.url!r}.",
        )
    except httpx.HTTPStatusError as e:
        LOGGER.error(
            f"Error response {e.response.status_code} while requesting {e.request.url!r}."
        )
        return e.response.status_code, f"HTTP Exception for {e.request.url!r} - {e}"
    except Exception as e:
        LOGGER.error(f"Error {e} for {url}")
        return 101, f"Error {e} for {url}"


async def save_to_db(db, url, domain, status, html, date):
    try:
        await db.execute(
            "INSERT INTO html (url, domain, status, html, date) VALUES (?, ?, ?, ?, ?)",
            (url, domain, status, html, date),
        )
        await db.commit()
        return 1
    except Exception as e:
        LOGGER.error(f"Error saving to db: {e}")
        return 0


async def fetch_and_save(session, db, domain, url, limiter):
    await db.execute(
        "CREATE TABLE IF NOT EXISTS html (url, domain, status, html, date)"
    )
    date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    async with limiter:
        status, html = await fetch_url(session, url)
        if html:
            result = await save_to_db(db, url, domain, status, html, date)
            return result
        else:
            result = await save_to_db(db, url, domain, status, "No HTML", date)
            return result


async def main(db_path, data):
    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "en-US,en;q=0.9",
        "Sec-Ch-Ua": '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"Windows"',
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    }
    # ssl_context = ssl.create_default_context(cafile=certifi.where())
    timeout = httpx.Timeout(2.0, connect=5.0)
    async with httpx.AsyncClient(
        headers=headers, timeout=timeout, verify=False, follow_redirects=True
    ) as session:
        limiter = AsyncLimiter(max_rate=5, time_period=0.1)
        async with aiosqlite.connect(db_path) as db:
            async with asyncio.TaskGroup() as tg:
                tasks = set()
                for domain in data["domain"]:
                    url = f"https://{domain}"
                    task = tg.create_task(
                        fetch_and_save(session, db, domain, url, limiter)
                    )
                    tasks.add(task)
            count = 0
            for t in tasks:
                res = await t
                count += res
        LOGGER.success(f"Saved successes {count} URLs to db")


"""
def get_headers_list():
    SCRAPEOPS_API_KEY = "8ece1d68-4d6e-4020-bba7-9f5650ce557a"
    response = requests.get(
        "http://headers.scrapeops.io/v1/browser-headers?api_key=" + SCRAPEOPS_API_KEY
    )
    json_response = response.json()
    return json_response.get("result", [])


def get_random_header(header_list):
    random_index = randint(0, len(header_list) - 1)
    return header_list[random_index]
"""

if __name__ == "__main__":
    directory = "/root/"
    db_path = directory + "domains.db"
    domain_file = "dns_input.parquet"
    # header_list = get_headers_list()
    LOGGER = create_logger()

    df = pl.read_parquet(directory + domain_file).select(["domain"])
    LOGGER.info(f"Shape: {df.shape} - {df.columns} Batches: {df.shape[0]/10000}")
    LOGGER.info("Started at %s", datetime.now())
    start = time.time()
    LOGGER.info("Starting at", datetime.now())
    for idx, frame in enumerate(df.iter_slices(n_rows=10000)):
        st = datetime.now()
        LOGGER.info(f"Starting batch {idx} at: {st}")
        asyncio.run(main(db_path, frame))
        et = datetime.now()
        LOGGER.info(f"Finished batch {idx} at: {et}")
    end = time.time()
    LOGGER.info(f("Finished at {end}"))
    LOGGER.info(f"Total time taken: {end-start} seconds")
