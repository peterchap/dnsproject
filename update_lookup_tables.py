import duckdb
import polars as pl
import requests
import tldextract
from zipfile import ZipFile
from io import BytesIO


def update_phishing_domains():
    # Update the phishing_domains table
    url2 = "https://raw.githubusercontent.com/mitchellkrogza/Phishing.Database/master/phishing-domains-ACTIVE.txt"
    df1 = pl.read_csv(url2, has_header=False, new_columns=["full_domain"])
    print(df1.shape)
    df1 = df1.with_columns(
        df1["full_domain"]
        .map_elements(lambda x: extract_domain(x), return_dtype=pl.String)
        .alias("domain")
    ).select(pl.col("domain").filter(pl.col("domain").is_in(pl.col("full_domain"))))
    # Add a new column 'is_phishing' to df2 and assign value 1 to all rows
    df1 = df1.with_columns(pl.lit(1).alias("is_phishing").cast(pl.Boolean))
    # Insert df2 into the phishing_domains table in duckdb
    conn.sql("CREATE OR REPLACE TABLE phishing_domains AS SELECT * FROM df1")
    print("Phishing domains updated")


def update_malware_domains():
    # Update the malware_domains table
    url3 = "https://raw.githubusercontent.com/stamparm/blackbook/master/blackbook.csv"
    df2 = pl.read_csv(url3, has_header=False, new_columns=["full_domain"])
    print(df2.shape)
    df2 = df2.with_columns(
        df2["full_domain"]
        .map_elements(lambda x: extract_domain(x), return_dtype=pl.String)
        .alias("domain")
    ).select(pl.col("domain").filter(pl.col("domain").is_in(pl.col("full_domain"))))
    # Add a new column 'is_malware' to df3 and assign value 1 to all rows
    df2 = df2.with_columns(pl.lit(1).alias("is_malware").cast(pl.Boolean))
    # Insert df3 into the malware_domains table in duckdb
    conn.sql("CREATE OR REPLACE TABLE malware_domains AS SELECT * FROM df2")
    print("Malware domains updated")


def update_mx_status_table():
    # Update the mx_status table
    data = directory + "mx_status.csv"
    df3 = pl.read_csv(data, has_header=True).cast(
        {
            "is_mailable": pl.Boolean,
            "is_disposable": pl.Boolean,
            "is_known_mbp": pl.Boolean,
        }
    )
    # Insert df3 into the mx_status table in duckdb
    conn.sql("CREATE OR REPLACE TABLE mx_status AS SELECT * FROM df3")
    print("MX status updated")


def update_webmail_table():
    # Update the webmail table
    data = directory + "webmail_domain_list.csv"
    df4 = pl.read_csv(data, has_header=True).cast({"is_webmail": pl.Boolean})
    # Insert df4 into the webmail table in duckdb
    conn.sql("CREATE OR REPLACE TABLE webmail AS SELECT * FROM df4")
    print("Webmail updated")


def update_mx_suffix_table():
    # Update the mx_suffix table
    data = directory + "mx_suffix_status_lookup.csv"
    df5 = pl.read_csv(data, has_header=True)
    # Insert df5 into the mx_suffix table in duckdb
    conn.sql("CREATE OR REPLACE TABLE mx_suffix AS SELECT * FROM df5")
    print("MX suffix updated")


def update_asn_lookup_table():
    # Update the asn_status table
    data = directory + "asn-ip4_ips.csv"
    df6 = pl.read_csv(data, has_header=True)
    # Insert df5 into the asn_status table in duckdb
    conn.sql("CREATE OR REPLACE TABLE asn_ip4 AS SELECT * FROM df6")
    print("ASN lookup updated")


def update_tld_lookup_table():
    # Update the tld_status table
    data = directory + "tld-lookup.csv"
    df7 = pl.read_csv(data, has_header=True)
    # Insert df7 into the tld_status table in duckdb
    conn.sql("CREATE OR REPLACE TABLE tld_status AS SELECT * FROM df7")
    print("TLD lookup updated")


def update_top_million():
    # Update the top-1m table
    url1 = "https://tranco-list.eu/top-1m.csv.zip"
    data = requests.get(url1)
    zip_file = ZipFile(BytesIO(data.content))

    df8 = pl.read_csv(
        zip_file.open("top-1m.csv").read(),
        has_header=False,
        new_columns=["top_domain_rank", "domain"],
    )
    print(df8.shape)
    # Insert df8 into the top-1m table in duckdb
    conn.sql("CREATE OR REPLACE TABLE top_1m AS SELECT * FROM df8")
    print("Top-1m updated")


def create_date_table():
    # Create the date table
    con.sql(
        "CREATE TABLE OR REPLACE create_date AS SELECT domain, create_date FROM read_parquet('/root/dnsresults/*.parquet')"
    )
    print("Date table created")


def extract_domain(domain):
    reg = extract(domain).registered_domain
    return reg


extract = tldextract.TLDExtract(include_psl_private_domains=True)
extract.update()
directory = "E:/domains-monitor/"
db_name = "test.duckdb"
conn = duckdb.connect(directory + db_name)
update_phishing_domains()
update_malware_domains()
update_webmail_table()
update_mx_suffix_table()
update_asn_lookup_table()
update_mx_status_table()
update_tld_lookup_table()
update_top_million()
# create_date_table()
print("All tables updated")
