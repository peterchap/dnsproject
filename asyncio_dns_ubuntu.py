""" use to list of domains to make DNS requests concurrently and save responses to disk."""
import asyncio
import dns.asyncresolver
import time
import pandas as pd
import pyarrow as pa
import csv
import boto3
import zipfile

from time import perf_counter as timer
from typing import List
from loguru import logger as custom_logger
from datetime import date
from pyarrow import csv

headers = {
    "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/118.0",
    "X-Amzn-Trace-Id": "Root=1-652d212a-22698af848e661fa00c61e34",
}

dns_provider = ["192.168.1.221"]
resolver = dns.asyncresolver.Resolver()
resolver.nameservers = dns_provider
resolver.lifetime = 3.0
resolver.timeout = 1.0


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
    directory = "/home/peter/Downloads/"
    custom_logger.remove()
    custom_logger.add(directory + "dnslog.log", colorize=True)
    return custom_logger


LOGGER = create_logger()
date = date.today().strftime("%Y-%m-%d")


async def execute_fetcher_tasks(urls_select: List[str], batch: int, total_count: int):
    start_time = timer()
    # print("batch: ", len(urls_to_fetch))
    async with asyncio.TaskGroup() as g:
        tasks = set()
        for i, url in enumerate(urls_select):
            task = g.create_task(fetch_url(url, batch, total_count, i))
            tasks.add(task)
        results = []
        keys = [
            "domain",
            "cname",
            "mx",
            "www",
            "wwwptr",
            "wwwcname",
            "mail",
            "mailptr",
            "date",
        ]
        for t in tasks:
            data = await t
            res = {keys[y]: data[y] for y in range(9)}
            results.append(res)
        df = pd.DataFrame(results)
        # (print("check ", df.shape))
    LOGGER.success(
        f"Executed Batch in {time.perf_counter() - start_time:0.2f} seconds."
    )
    return df


async def fetch_url(domain: str, batch: int, total_count: int, i: int):
    """
    Fetch raw HTML from a URL prior to parsing.
    :param ClientSession session: Async HTTP requests session.
    :param str url: Target URL to be fetched.
    :param AsyncIOFile outfile: Path of local file to write to.
    :param int total_count: Total number of URLs to be fetched.
    :param int i: Current iteration of URL out of total URLs.
    """
    cname = await get_cname(domain)
    mx = await get_mx(domain)
    www, wwwptr, wwwcname = await get_www(domain)
    mail, mailptr = await get_mail(domain)

    LOGGER.info(f"Processed {batch +i+1} of {total_count} URLs.")

    return [domain, cname, mx, www, wwwptr, wwwcname, mail, mailptr, date]


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


def read_file_from_s3(bucket_name, file_name):
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket="domain-monitor", Key=file_name)
    data = obj["Body"].read().decode("utf-8")
    return data


if __name__ == "__main__":
    directory = "/root/dns_project/"
    bucket_name = "domain-monitor"
    file_key = "domain_update_daily2023-10-16-05-27-18.zip"

    # Path to save the downloaded zip file (this could be /tmp if you're running on AWS Lambda)
    download_path = "/root/dns_project/"
    extract_dir = "/root/dns_project/"
    csv_file_name = "domains-detailed-update.csv"

    session = boto3.session.Session()
    client = session.client("s3")
    client.download_file(bucket_name, file_key, directory + file_key)
    # region_name='your-region',  # e.g., us-west-1
    # aws_access_key_id='your-access-key',
    # aws_secret_access_key='your-secret-key')
    with zipfile.ZipFile(directory + file_key, "r") as zip_ref:
        for member in zip_ref.namelist():
            if csv_file_name in member:
                zip_ref.extract(member, path=extract_dir)
                break

    cols = ["domain", "ns", "ip", "country", "web_server", "Alexa_rank"]

    read_options = csv.ReadOptions(
        use_threads=True, autogenerate_column_names=True, encoding="utf-8"
    )
    parse_options = csv.ParseOptions(
        delimiter=";",
        quote_char='"',
        ignore_empty_lines=False,
    )
    table = csv.read_csv(
        directory + csv_file_name,
        read_options=read_options,
        parse_options=parse_options,
    )
    table = table.drop_columns(["f5", "f7"])
    table = table.rename_columns(cols)
    # df = pd.read_parquet(directory + file, engine='auto', columns=['name'])
    # print('df', df.shape)
    urls_to_fetch = table["domain"].to_pylist()
    print(len(urls_to_fetch))
    len = len(urls_to_fetch)
    start_time = time.time()
    start = 0
    # end = 0
    keys = [
        "domain",
        "cname",
        "mx",
        "www",
        "wwwptr",
        "wwwcname",
        "mail",
        "mailptr",
        "date",
    ]
    t2 = pa.string()
    schema = pa.schema(
        [
            pa.field("domain", pa.string()),
            pa.field("cname", pa.list_(t2)),
            pa.field("mx", pa.list_(t2)),
            pa.field("www", pa.list_(t2)),
            pa.field("wwwptr", pa.list_(t2)),
            pa.field("wwwcname", pa.list_(t2)),
            pa.field("mail", pa.list_(t2)),
            pa.field("mailptr", pa.list_(t2)),
            pa.field("date", pa.string()),
        ]
    )
    with pa.OSFile(directory + "domains_all.arrow", "wb") as sink:
        with pa.ipc.new_stream(sink, schema) as writer:
            start = 0
            batchcount = 0
            step = 10000
            for i in range(0, len, step):
                df = asyncio.run(
                    execute_fetcher_tasks(
                        urls_to_fetch[start : i + step], batchcount, len
                    )
                )
                batch = pa.RecordBatch.from_pandas(df)
                writer.write_batch(batch)
                start = i + step
                batchcount = batchcount + step
    LOGGER.success(f"Executed Batch in {time.time() - start_time:0.2f} seconds.")
    print("Elapsed time: ", time.time() - start_time)
