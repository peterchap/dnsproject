import asyncio
import dns.asyncresolver
import time
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import re
import csv
import os
import tldextract
import datetime
from time import perf_counter as timer
from typing import List
from loguru import logger as custom_logger
from datetime import date
from pyarrow import csv
from io import BytesIO
from aiolimiter import AsyncLimiter

headers = {
    "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/119.0"
}

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


async def execute_fetcher_tasks(urls_select: List[str],batchcount: int,  total_count: int):
    # start_time = timer()
    limiter = AsyncLimiter(100, 1)
    async with asyncio.TaskGroup() as g:
        tasks = set()
        for i, url in enumerate(urls_select):
            async with limiter:
                task = g.create_task(fetch_url(url,  total_count))
                tasks.add(task)
        results = []
        keys = [
            "domain",
            "a",
            "cname",
            "mx",
            "mx_domain",
            "spf",
            "www",
            "wwwptr",
            "wwwcname",
            "mail",
            "mailptr",
            "refresh_date",
        ]
        for t in tasks:
            data = await t
            res = {keys[y]: data[y] for y in range(12)}
            results.append(res)
        df = pd.DataFrame(results)
        df['refresh_date'] = pd.to_datetime(df['refresh_date'])                                   
        # (print("check ", df.shape))
        LOGGER.success(
          f"Executed Batch of {batchcount} in {time.perf_counter() - start_time:0.2f} seconds.")
    return df


async def fetch_url(domain: str,  total_count: int):
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
    mx, mx_domain = await get_mx(domain)
    spf = await get_spf(domain)
    www, wwwptr, wwwcname = await get_www(domain)
    mail, mailptr = await get_mail(domain)
    refresh_date = datetime.datetime.now()

    # LOGGER.info(f"Processed {batch +i+1} of {total_count} URLs.")

    return [
        domain,
        a,
        cname,
        mx,
        mx_domain,
        spf,
        www,
        wwwptr,
        wwwcname,
        mail,
        mailptr,
        refresh_date,
    ]


def listToString(list):
    join = ","
    str1 = ""
    for ele in list:
        ele1 = ele.replace(",", ":").rstrip(".")
        str1 += ele1 + join
    return str1

def extract_registered_domain(mx_record):
    result = extract(mx_record).registered_domain
    return f"{result}"

async def get_A(domain):
    try:
        result = await resolver.resolve(domain, "A")
        a = []
        for rr in result:
            a.append(rr.to_text())
        a = listToString(a).rstrip(",")
    except Exception as e:
        a = "No A"
    return a


async def get_cname(domain):
    try:
        answers = await resolver.resolve(domain, "CNAME")
        cname = []
        for cn in answers:
            cname.append(cn.to_text().rstrip("."))
        cname = listToString(cname).rstrip(",")
    except dns.resolver.NoAnswer as e:
        cname = "No CNAME"
    except Exception as e:
        cname = "Null"
    return cname


async def get_mx(domain):
    try:
        result = await resolver.resolve(domain, "MX")
        mx = []
        for rr in result:
            mx.append(f"{rr.preference}, {rr.exchange}")
        mx = listToString(mx).rstrip(",")
        split = mx.split(":")[1].strip().split(",")[0]
        mx_domain = extract_registered_domain(split)
    except dns.resolver.NoAnswer as e:
        mx = "No MX"
        mx_domain = None
    except Exception as e:
        mx = "Null"
        mx_domain = None
    return mx, mx_domain

async def get_www(domain):
    try:
        result = await resolver.resolve("www." + domain)
        www = []
        for rr in result:
            www.append(rr.to_text())
        www = listToString(www).rstrip(",")
        try:
            wwwptr = []
            for ip in www:
                res = await resolver.resolve_address(ip)
                for rr in res:
                    wwwptr.append(rr.to_text().rstrip("."))
                wwwptr = listToString(wwwptr).rstrip(",")
        except dns.resolver.NoAnswer as e:
            wwwptr = "No Answer"
        except Exception as e:
            wwwptr = "Null"
        try:
            result = await resolver.resolve("www." + domain, "CNAME")
            wwwcname = []
            for wwwcn in result:
                wwwcname.append(wwwcn.to_text().rstrip("."))
            wwwcname = listToString(wwwcname).rstrip(",")
        except dns.resolver.NoAnswer as e:
            wwwcname = "No Answer"
        except Exception as e:
            wwwcname = "Null"
    except dns.resolver.NoAnswer as e:
        www = "No Answer"
        wwwptr = "No Answer"
        wwwcname = "No Answer"
    except Exception as e:
        www = "Null"
        wwwptr = "Null"
        wwwcname = "Null"

    return www, wwwcname, wwwptr


async def get_mail(domain):
    try:
        result = await resolver.resolve("mail." + domain, "A")
        mail = []
        for rr in result:
            mail.append(rr.to_text())
        mail = listToString(mail).rstrip(",")
        try:
            mailptr = []
            res = await resolver.resolve_address(mail[0])
            for rr in res:
                mailptr.append(rr.to_text())
            mailptr = listToString(mailptr).rstrip(",")
        except dns.resolver.NoAnswer as e:
            mailptr = "No Answer"
        except Exception as e:
            mailptr = "Null"

    except dns.resolver.NoAnswer as e:
        mail = "No Answer"
        mailptr = "No Answer"
    except Exception as e:
        mail = "Null"
        mailptr = "Null"
    return mail, mailptr


async def get_spf(domain):
    try:
        result = await resolver.resolve(domain, "TXT")
        spf = None
        for rr in result:
            # print(domain, rr.text)
            if "spf" in rr.text.lower():
                spf = rr.to_text().strip('"')
        spf = listToString(spf).rstrip(",")
        if spf is None:
            spf = "No SPF"
    except Exception as e:
        spf = "Null"
    return spf

if __name__ == "__main__":
    directory = "/root/dnsproject/"
    #bucket_name = "domain-monitor-results"
    file_key = "dns_input.parquet"
    table = pq.read_table(directory + file_key)
    allurls = table["domain"].to_pylist()
    urls_to_fetch = [value.rstrip(".") if isinstance(value, str) else value for value in allurls]
    print(len(urls_to_fetch))
    len = len(urls_to_fetch)
    start_time = time.time()
    start = 0
    # end = 0
    schema = pa.schema(
        [
            pa.field("domain", pa.string()),
            pa.field("a", pa.string()),
            pa.field("cname", pa.string()),
            pa.field("mx", pa.string()),
            pa.field("mx_domain", pa.string()),
            pa.field("spf", pa.string()),
            pa.field("www", pa.string()),
            pa.field("wwwptr", pa.string()),
            pa.field("wwwcname", pa.string()),
            pa.field("mail", pa.string()),
            pa.field("mailptr", pa.string()),
            pa.field("refresh_date", pa.timestamp('ns')),
        ]
    )
    with pa.OSFile(directory + "domains_all.arrow", "wb") as sink:
        with pa.ipc.new_stream(sink, schema) as writer:
            start = 0
            batchcount = 0
            step = 50000
            for i in range(0, len, step):
                df = asyncio.run(
                    execute_fetcher_tasks(
                        urls_to_fetch[start : i + step], batchcount, len
                    )
                )
                batch = pa.RecordBatch.from_pandas(df, schema=schema)
                writer.write_batch(batch)
                start = i + step
                batchcount = batchcount + step
                LOGGER.success(f"Executed Batch in {time.time() - start_time:0.2f} seconds.")
    print("Elapsed time: ", time.time() - start_time)
