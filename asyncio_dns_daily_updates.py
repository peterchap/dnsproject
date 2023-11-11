import asyncio
import dns.asyncresolver
import time
import pandas as pd
import re

import os
import datetime

from time import perf_counter as timer
from typing import List
from loguru import logger as custom_logger
from datetime import date
from aiolimiter import AsyncLimiter

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "X-Amzn-Trace-Id": "Root=1-6549623d-0da03b1f0eb65a331f8bd46c",
}

# dns_provider = ["192.168.1.221"]
dns_provider = ["127.0.0.1"]
resolver = dns.asyncresolver.Resolver()
resolver.nameservers = dns_provider
resolver.lifetime = 2.0
resolver.timeout = 2.0


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
    directory = "/root/dnsproject/"
    custom_logger.remove()
    custom_logger.add(directory + "dnslog.log", colorize=True)
    return custom_logger


LOGGER = create_logger()
date = date.today().strftime("%Y-%m-%d")


async def execute_fetcher_tasks(urls_select: List[str], filename, total_count: int):
    # start_time = timer()
    limiter = AsyncLimiter(100, 1)
    async with asyncio.TaskGroup() as g:
        tasks = set()
        for i, url in enumerate(urls_select):
            async with limiter:
                task = g.create_task(fetch_url(url, total_count, i))
                tasks.add(task)
        results = []
        keys = [
            "domain",
            "a",
            "cname",
            "mx",
            "spf",
            "www",
            "wwwptr",
            "wwwcname",
            "mail",
            "mailptr",
            "create_date",
            "refresh_date",
        ]
        for t in tasks:
            data = await t
            res = {keys[y]: data[y] for y in range(12)}
            results.append(res)
        df = pd.DataFrame(results)
        df['domain_date'] = pd.to_datetime(df['domain_date'])
        df['create_date'] = pd.to_datetime(df['create_date'])                                   
        # (print("check ", df.shape))
        # LOGGER.success(
        #  f"Executed Batch in {time.perf_counter() - start_time:0.2f} seconds.")
    return df


async def fetch_url(domain: str, filename, total_count: int):
    """
    Fetch raw HTML from a URL prior to parsing.
    :param ClientSession session: Async HTTP requests session.
    :param str url: Target URL to be fetched.
    :param AsyncIOFile outfile: Path of local file to write to.
    :param int total_count: Total number of URLs to be fetched.
    :param int i: Current iteration of URL out of total URLs.
    """

    valid_pattern = re.compile(r"[^a-zA-Z0-9.-]")
    domain = valid_pattern.sub("", domain)
    a = await get_A(domain)
    cname = await get_cname(domain)
    mx = await get_mx(domain)
    spf = await get_spf(domain)
    www, wwwptr, wwwcname = await get_www(domain)
    mail, mailptr = await get_mail(domain)
    date = get_create_date(filename)
    updated = datetime.datetime.now()

    # LOGGER.info(f"Processed {batch +i+1} of {total_count} URLs.")

    return [
        domain,
        a,
        cname,
        mx,
        spf,
        www,
        wwwptr,
        wwwcname,
        mail,
        mailptr,
        date,
        updated,
    ]


async def get_A(domain):
    try:
        result = await resolver.resolve(domain, "A")
        a = []
        for rr in result:
            a.append(rr.to_text())
    except Exception as e:
        a = ["No A"]
    return a


async def get_cname(domain):
    try:
        answers = await resolver.resolve(domain, "CNAME")
        cname = []
        for cn in answers:
            cname.append(cn.to_text().rstrip("."))
    except dns.resolver.NoAnswer as e:
        cname = ["No CNAME"]
    except Exception as e:
        cname = ["Null"]
    return cname


async def get_mx(domain):
    try:
        result = await resolver.resolve(domain, "MX")
        mx = []
        for rr in result:
            mx.append(f"{rr.preference}, {rr.exchange}")
    except dns.resolver.NoAnswer as e:
        mx = ["No MX"]
    except Exception as e:
        mx = ["Null"]
    return mx


async def get_www(domain):
    try:
        result = await resolver.resolve("www." + domain)
        www = []
        for rr in result:
            www.append(rr.to_text())
        try:
            wwwptr = []
            for ip in www:
                res = await resolver.resolve_address(ip)
                for rr in res:
                    wwwptr.append(rr.to_text().rstrip("."))
        except dns.resolver.NoAnswer as e:
            wwwptr = ["No Answer"]
        except Exception as e:
            wwwptr = ["Null"]
        try:
            result = await resolver.resolve("www." + domain, "CNAME")
            wwwcname = []
            for wwwcn in result:
                wwwcname.append(wwwcn.to_text().rstrip("."))
        except dns.resolver.NoAnswer as e:
            wwwcname = ["No Answer"]
        except Exception as e:
            wwwcname = ["Null"]
    except dns.resolver.NoAnswer as e:
        www = ["No Answer"]
        wwwptr = ["No Answer"]
        wwwcname = ["No Answer"]
    except Exception as e:
        www = ["Null"]
        wwwptr = ["Null"]
        wwwcname = ["Null"]

    return www, wwwcname, wwwptr


async def get_mail(domain):
    try:
        result = await resolver.resolve("mail." + domain, "A")
        mail = []
        for rr in result:
            mail.append(rr.to_text())
        try:
            mailptr = []
            res = await resolver.resolve_address(mail[0])
            for rr in res:
                mailptr.append(rr.to_text())
        except dns.resolver.NoAnswer as e:
            mailptr = ["No Answer"]
        except Exception as e:
            mailptr = ["Null"]

    except dns.resolver.NoAnswer as e:
        mail = ["No Answer"]
        mailptr = ["No Answer"]
    except Exception as e:
        mail = ["Null"]
        mailptr = ["Null"]
    return mail, mailptr


async def get_spf(domain):
    try:
        result = await resolver.resolve(domain, "TXT")
        spf = None
        for rr in result:
            # print(domain, rr.text)
            if "spf" in rr.text.lower():
                spf = [rr.text]
        if spf is None:
            spf = ["No SPF"]
    except Exception as e:
        spf = ["Null"]
    return spf


def get_create_date(filename):
    date_format = "%Y-%m-%d"
    x = filename.split(".")[0]
    b = x[19:29]
    date = datetime.datetime.strptime(b, date_format)
    return date


if __name__ == "__main__":
    directory = "/root/updates/"
    output = "/root/dnsproject/"
    #directory = "/home/peter/Documents/updates/"
    #output = "/home/peter/Documents/dnsproject/"
    start_time = time.time()

    # download_path = "/home/peter/Downloads/"
    # extract_dir = "/home/peter/Downloads/"
    final = pd.DataFrame()
    for file in os.listdir(directory):
        df = pd.read_parquet(directory + file, engine="pyarrow", columns=["domain"])
        urls_to_fetch = df["domain"].tolist()
        len = df.shape[0]
        df = asyncio.run(execute_fetcher_tasks(urls_to_fetch, file, len))
        final = pd.concat([final, df])
        print("check ", final.shape)
        LOGGER.success(f"Executed Batch in {time.time() - start_time:0.2f} seconds.")
    final.to_parquet(output + "domains_updates.parquet", engine= 'fastparquet')
    LOGGER.success(f"completed in {time.time() - start_time:0.2f} seconds.")
    print("Elapsed time: ", time.time() - start_time)

    # read in arrow file
