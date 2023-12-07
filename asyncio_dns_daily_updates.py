import asyncio
import dns.asyncresolver
import time
import pandas as pd
import re
import tldextract
import os
import datetime
import pyarrow as pa
import socket
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
            "wwwptr",
            "wwwcname",
            "mail_a",
            "mail_mx",
            "mail_mx_domain",
            "mail_mx_suffix",
            "mail_spf",
            "mail_dmarc",
            "mail_ptr",
            "create_date",
            "refresh_date",
        ]
        for t in tasks:
            data = await t
            res = {keys[y]: data[y] for y in range(22)}
            results.append(res)
        df = pd.DataFrame(results)
        df["create_date"] = pd.to_datetime(df["create_date"])
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
    create_date = await get_create_date(filename)
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
        create_date,
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


async def get_create_date(filename):
    date_format = "%Y-%m-%d"
    x = filename.split(".")[0]
    b = x[19:29]
    date = str(datetime.datetime.strptime(b, date_format))
    return date


if __name__ == "__main__":
    print("Starting...")
    directory = "/root/updates/"
    output = "/root/dnsresults/"
    #directory = "E:/domains-monitor/updates/"
    extract = tldextract.TLDExtract(include_psl_private_domains=True)
    extract.update()
    #directory = "/home/peter/Documents/updates/"
    #output = "/home/peter/Documents/dnsproject/"
    start_time = time.time()

    #download_path = "/home/peter/Downloads/"
    #extract_dir = "/home/peter/Downloads/"
    final = pd.DataFrame()
    for file in os.listdir(directory):
        print(file)
        df = pd.read_parquet(directory + file, engine="pyarrow", columns=["domain"])
        urls_to_fetch = df["domain"].tolist()
        len = df.shape[0]
        df = asyncio.run(execute_fetcher_tasks(urls_to_fetch, file, len))
        final = pd.concat([final, df])
        print("check ", final.shape)
        LOGGER.success(f"Executed Batch in {time.time() - start_time:0.2f} seconds.")
    final.to_parquet(output + "domains_updates.parquet")
    LOGGER.success(f"completed in {time.time() - start_time:0.2f} seconds.")
    print("Elapsed time: ", time.time() - start_time)

