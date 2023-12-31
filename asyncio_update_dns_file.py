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
    directory = "/root/"
    #directory = "/home/peter/Documents/updates/"
    custom_logger.remove()
    custom_logger.add(directory + "dnslog.log", colorize=True)
    return custom_logger


LOGGER = create_logger()
date = datetime.datetime.now().strftime("%Y-%m-%d")


async def execute_fetcher_tasks(
    urls_select: List[str], filename: str, total_count: int
):
    # start_time = timer()
    limiter = AsyncLimiter(120, 1)
    async with asyncio.TaskGroup() as g:
        tasks = set()
        for i, url in enumerate(urls_select):
            async with limiter:
                task = g.create_task(fetch_url(url, filename, total_count))
                tasks.add(task)
        results = []
        keys = [
            "domain",
            "suffix",
            "a",
            "ptr",
            "cname",
            "mx",
            "mx_domain",
            "mx_suffix",
            "spf",
            "dmarc",
            "www",
            "www_ptr",
            "www_cname",
            "mail_a",
            "mail_mx",
            "mail_mx_domain",
            "mail_mx_suffix",
            "mail_spf",
            "mail_dmarc",
            "mail_ptr",
            "refresh_date",
        ]
        for t in tasks:
            data = await t
            res = {keys[y]: data[y] for y in range(21)}
            results.append(res)
        df = pd.DataFrame(results)
        df["refresh_date"] = pd.to_datetime(df["refresh_date"])
        # (print("check ", df.shape))
        # LOGGER.success(
        #  f"Executed Batch in {time.perf_counter() - start_time:0.2f} seconds.")
    return df


async def fetch_url(domain: str, filename: str, total_count: int):
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
    suffix = extract_suffix(domain)
    a = await get_A(domain)
    if a  == "None":
       ptr  = "None"
    else:
       ptr = await get_ptr(a.split(", ")[0])
    cname = await get_cname(domain)
    mx, mx_domain, mx_suffix = await get_mx(domain)
    if mx == "None":
       spf = "None"
       dmarc = "None"
    else:
       spf = await get_spf(domain)
       dmarc = await get_dmarc(domain)
    www, www_ptr, www_cname = await get_www(domain)
    (
        mail_a,
        mail_mx,
        mail_mx_domain,
        mail_suffix,
        mail_spf,
        mail_dmarc,
        mail_ptr,
    ) = await get_mail(domain)
    refresh_date = datetime.datetime.now()

    # LOGGER.info(f"Processed {batch +i+1} of {total_count} URLs.")

    return [
        domain,
        suffix,
        a,
        ptr,
        cname,
        mx,
        mx_domain,
        mx_suffix,
        spf,
        dmarc,
        www,
        www_ptr,
        www_cname,
        mail_a,
        mail_mx,
        mail_mx_domain,
        mail_suffix,
        mail_spf,
        mail_dmarc,
        mail_ptr,
        refresh_date,
    ]


def mxToString(list):
    join = ", "
    str1 = ""
    for ele in list:
        ele1 = ele.replace(",", ":").rstrip(".")
        str1 += ele1 + join
    return str1


def listToString(list):
    join = ", "
    str1 = ""
    for ele in list:
        str1 += ele + join
    str1 = str1.rstrip(", ")
    return str1


def extract_registered_domain(mx_record):
    reg = extract(mx_record).registered_domain
    suffix = extract(mx_record).suffix
    return reg, suffix


def extract_suffix(domain):
    result = extract(domain).suffix
    return result


async def get_A(domain):
    try:
        result = await resolver.resolve(domain, "A")
        a = []
        for rr in result:
            a.append(rr.to_text())
        a = listToString(a).rstrip(",")
    except Exception as e:
        a = "None"
    return a


async def get_ns(domain):
    try:
        result = await resolver.resolve(domain, "NS")
        ns = []
        for rr in result:
            ns.append(rr.to_text().rstrip("."))
        ns = listToString(ns).rstrip(",")
    except Exception as e:
        ns = e
    return ns


async def get_cname(domain):
    try:
        answers = await resolver.resolve(domain, "CNAME")
        cname = []
        for cn in answers:
            cname.append(cn.to_text().rstrip("."))
        cname = listToString(cname).rstrip(",")
    except dns.resolver.NoAnswer as e:
        cname = "None"
    except Exception as e:
        cname = "None"
    return cname


async def get_mx(domain):
    try:
        result = await resolver.resolve(domain, "MX")
        mx = []
        for rr in result:
            mx.append(f"{rr.preference}, {rr.exchange}")
        mx = mxToString(mx).rstrip(", ")
        split = mx.split(":")[1].strip().split(",")[0]
        mx_domain, suffix = extract_registered_domain(split)
    except Exception as e:
        mx = "None"
        mx_domain = None
        suffix = None
    return mx, mx_domain, suffix


async def get_ptr(ip):
    try:
        result = await resolver.resolve_address(ip)
        for rr in result:
          ptr = (f"{rr}")
        if ptr == ip:
            ptr = "None"
    except Exception as e:
        ptr = "None"
    return ptr


async def get_www(domain):
    www = await get_A("www." + domain)
    if www == "None":
        www_ptr = "None"
    else:
        www_ptr = await get_ptr(www.split(", ")[0])
    www_cname = await get_cname("www." + domain)

    return www, www_ptr, www_cname


async def get_mail(domain):
    mail_a = await get_A("mail." + domain)
    mail_mx, mail_mx_domain, mail_suffix = await get_mx("mail." + domain)
    if mail_a != "None":
        mail_ptr = await get_ptr(mail_a.split(", ")[0])
    else:
        mail_ptr = "None"
    if mail_mx == "None":
        mail_spf = "None"
        mail_dmarc = "None"
    else:
        mail_spf = await get_spf("mail." + domain)
        mail_dmarc = await get_dmarc("mail." + domain)

    return mail_a, mail_mx, mail_mx_domain, mail_suffix, mail_spf, mail_dmarc, mail_ptr


async def get_spf(domain):
    try:
        result = await resolver.resolve(domain, "TXT")
        spf = None
        for rr in result:
            if "spf" in rr.to_text().lower():
                spf = rr.to_text().strip('"')
        if spf is None:
            spf = "None"
    except Exception as e:
        spf = "None"
    return spf


async def get_dmarc(domain):
    try:
        result = await resolver.resolve("_dmarc." + domain, "TXT")
        dmarc = None
        for rr in result:
            if "dmarc" in rr.to_text().lower():
                dmarc = rr.to_text().strip('"')
        if dmarc is None:
            dmarc = "None"
    except Exception as e:
        dmarc = "None"
    return dmarc



if __name__ == "__main__":
    directory = "/root/"
    extract = tldextract.TLDExtract(include_psl_private_domains=True)
    extract.update()
    # bucket_name = "domain-monitor-results"
    file_key = "dns_input.parquet"
    table = pq.read_table(directory + file_key)
    allurls = table["domain"].to_pylist()
    urls_to_fetch = [
        value.rstrip(".") if isinstance(value, str) else value for value in allurls
    ]
    print(len(urls_to_fetch))
    len = len(urls_to_fetch)
    start_time = time.time()
    start = 0
    # end = 0
    schema = pa.schema(
        [
            pa.field("domain", pa.string()),
            pa.field("suffix", pa.string()),
            pa.field("a", pa.string()),
            pa.field("ptr", pa.string()),
            pa.field("cname", pa.string()),
            pa.field("mx", pa.string()),
            pa.field("mx_domain", pa.string()),
            pa.field("mx_suffix", pa.string()),
            pa.field("spf", pa.string()),
            pa.field("dmarc", pa.string()),
            pa.field("www", pa.string()),
            pa.field("www_ptr", pa.string()),
            pa.field("www_cname", pa.string()),
            pa.field("mail_a", pa.string()),
            pa.field("mail_mx", pa.string()),
            pa.field("mail_mx_domain", pa.string()),
            pa.field("mail_mx_suffix", pa.string()),
            pa.field("mail_spf", pa.string()),
            pa.field("mail_dmarc", pa.string()),
            pa.field("mail_ptr", pa.string()),
            pa.field("refresh_date", pa.timestamp("ns")),
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
                LOGGER.success(
                    f"Executed Batch in {time.time() - start_time:0.2f} seconds."
                )
    print("Elapsed time: ", time.time() - start_time)
