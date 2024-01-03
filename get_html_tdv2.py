import asyncio
import duckdb
import httpx
import polars as pl
import pandas as pd
import time
from aiolimiter import AsyncLimiter
from bs4 import BeautifulSoup as bs4
from datetime import datetime
from loguru import logger as custom_logger
from googletrans import Translator
from sys import stdout

# logdir = "/root/"
logdir = "E:/domains-monitor/"
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


async def fetch_url(session, url, domain, date):
    try:
        html = await session.get(url)
        LOGGER.info(f"url: {url} - status: {html.status_code}")
        data = await parse_html(html.text, domain)
        return data
    except httpx.ConnectError as e:
        LOGGER.error(f"Connection error for {e.request.url!r}.")
        data = {
            "domain": domain,
            "status": "102",
            "title": "Connection error",
            "desc": f"{e.request.url!r} - {e}",
            "parked": None,
            "language": None,
            "translated": None,
            "refresh_date": date,
        }
        LOGGER.error(f"Connection error for {e.request.url!r}.")
        return data
    except httpx.ConnectTimeout as e:
        data = {
            "domain": domain,
            "status": "103",
            "title": "Connection timeout",
            "desc": f"{e.request.url!r} - {e}",
            "parked": None,
            "language": None,
            "translated": None,
            "refresh_date": date,
        }
        LOGGER.error(f"Connection timeout for {e.request.url!r}.")
        return data
    except httpx.RequestError as e:
        data = {
            "domain": domain,
            "status": "104",
            "title": "Request error block",
            "desc": f"{e} while requesting {e.request.url!r}",
            "parked": None,
            "language": None,
            "translated": None,
            "refresh_date": date,
        }
        LOGGER.error(f"{e} - {e.request.url!r}.")
        return data
    except httpx.HTTPStatusError as e:
        data = {
            "domain": domain,
            "status": "105",
            "title": "HTTP status error",
            "desc": f"{e} while requesting {e.request.url!r}",
            "parked": None,
            "language": None,
            "translated": None,
            "refresh_date": date,
        }
        LOGGER.error(f"{e} - {e.request.url!r}.")
        return data
    except Exception as e:
        data = {
            "domain": domain,
            "status": "101",
            "title": "Other exception error",
            "desc": f"Error {e} for {url}",
            "parked": None,
            "language": None,
            "translated": None,
            "refresh_date": date,
        }
        LOGGER.error(f"Error {e} for {url}")
        return data


async def parse_html(html, domain):
    date = datetime.now().strftime("%Y-%m-%d")
    soup = bs4(html, "html.parser")
    if soup.title:
        title = soup.title.text
    elif ((soup.find("meta", attrs={"property": "og:title"}))) is not None:
        title = soup.find("meta", attrs={"property": "og:title"}).get("content")
    else:
        title = "No Title"

    if (
        (
            soup.find(
                "meta",
                attrs={"property": lambda x: x and x.lower() == "description"},
            )
        )
    ) is not None:
        desc = soup.find(
            "meta", attrs={"property": lambda x: x and x.lower() == "description"}
        ).get("content")
    elif (
        (
            soup.find(
                "meta",
                attrs={"property": lambda x: x and x.lower() == "og:description"},
            )
        )
    ) is not None:
        desc = soup.find(
            "meta",
            attrs={"property": lambda x: x and x.lower() == "og:description"},
        ).get("content")
    elif (
        soup.find("meta", attrs={"name": lambda x: x and x.lower() == "description"})
    ) is not None:
        desc = soup.find(
            "meta", attrs={"name": lambda x: x and x.lower() == "description"}
        ).get("content")
    else:
        desc = "No Description"
    #
    text = str(title).lower() + " " + str(desc).lower()

    language, transalated = await language_check(text)

    park = park_check(str(soup), domain)
    if title:
        title = title.replace(",", " ")[:75]
    else:
        title = "No Title"
    if desc:
        desc = desc.replace(",", " ")[:200]
    else:
        desc = "No Description"
    data = {
        "domain": domain,
        "status": "200",
        "title": title,
        "desc": desc,
        "parked": park,
        "language": language,
        "translated": transalated,
        "refresh_date": date,
    }
    return data


def park_check(soup1, domain):
    parkwords = [
        "domain for sale",
        "domain parking",
    ]
    if f"The domain name {domain} is for sale" in soup1:
        park = "Parked1"
    elif "window.park" in soup1:
        park = "Parked2"
    elif f"{domain} domain name is for sale. Inquire now." in soup1:
        park = "Parked3"
    elif "This domain {domain} may be for sale!" in soup1:
        park = "Parked4"
    elif any(word in str(soup1) for word in parkwords):
        park = "Parked5"
    else:
        park = "Not Parked"
    return park


async def language_check(text):
    translator = Translator()
    language = translator.detect(text).lang
    if language != "en":
        translated = translator.translate(text).text.lower()
    else:
        translated = text
    return language, translated


def save_to_db(db, results):
    table_name = "html"
    db.execute(
        "CREATE TABLE IF NOT EXISTS html (domain VARCHAR, status VARCHAR, title VARCHAR, description VARCHAR, parked VARCHAR, language VARCHAR, transalation VARCHAR, refresh_date DATE)"
    )
    df = pl.from_dicts(results)
    db.sql(f"INSERT INTO {table_name} SELECT * FROM df")
    return len(results)


async def main(domains):
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
    """Create tasks and execute them."""
    timeout = httpx.Timeout(2.0, connect=5.0)
    date = datetime.now().strftime("%Y-%m-%d")
    async with httpx.AsyncClient(
        headers=headers, timeout=timeout, verify=False, follow_redirects=True
    ) as session:
        limiter = AsyncLimiter(max_rate=4, time_period=0.1)
        async with asyncio.TaskGroup() as g:
            tasks = set()
            for i, domain in enumerate(domains["domain"]):
                async with limiter:
                    url = f"https://{domain}"
                    task = g.create_task(fetch_url(session, url, domain, date=date))
                    tasks.add(task)
            results = []
            for t in tasks:
                res = await t
                results.append(res)
    LOGGER.success(f"Saved successes {len(results)} URLs to db")
    return results


if __name__ == "__main__":
    # directory = "/root/"
    directory = "E:/domains-monitor/"
    db_path = directory + "domains.duckdb"
    domain_file = "contabo1_domains_redone.parquet"
    # domain_file = "dns_input.parquet"
    # header_list = get_headers_list()
    LOGGER = create_logger()
    db = duckdb.connect(db_path)
    df = pl.read_parquet(directory + domain_file).select(["domain"])
    LOGGER.info(f"Shape: {df.shape} - {df.columns} Batches: {df.shape[0]/10000}")
    LOGGER.info(f"Started at {datetime.now()}")
    start = time.time()
    LOGGER.info(f"Starting at  {datetime.now()}")
    for idx, frame in enumerate(df.iter_slices(n_rows=100000)):
        st = datetime.now()
        LOGGER.info(f"Starting batch {idx} at: {st}")
        batch = asyncio.run(main(frame))
        print(batch)
        res = save_to_db(db, batch)
        date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        LOGGER.success(f"Saved successes {res} URLs to db")
        et = datetime.now()
        LOGGER.info(f"Finished batch {idx} at: {et}")
    end = time.time()
    LOGGER.info(f"Finished at {end}")
    LOGGER.info(f"Total time taken: {end-start} seconds")
