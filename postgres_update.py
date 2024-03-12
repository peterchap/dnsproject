import duckdb
import timeit
import glob

directory = "~/"
directory2 = "~/refresh/"
db = "domains_all_postgres.duckdb"

con = duckdb.connect(directory + db)
# DESCRIBEfiles = glob.glob(directory2 + "*.parquet")

stage1 = """CREATE TABLE IF NOT EXISTS domains_stage1 (
    domain VARCHAR PRIMARY KEY,
    ns VARCHAR,
    ip VARCHAR,
    country_dm VARCHAR,
    suffix VARCHAR,
    a VARCHAR,
    ip_int BIGINT,
    ptr VARCHAR,
    cname VARCHAR,
    mx VARCHAR,
    mx_domain VARCHAR,
    mx_suffix VARCHAR,
    spf VARCHAR,
    dmarc VARCHAR,
    www VARCHAR,
    www_ptr VARCHAR,
    www_cname VARCHAR,
    mail_a VARCHAR,
    mail_mx VARCHAR,
    mail_mx_domain VARCHAR,
    mail_mx_suffix VARCHAR,
    mail_spf VARCHAR,
    mail_dmarc VARCHAR,
    mail_ptr VARCHAR,
    refresh_date DATETIME)"""

query = """CREATE OR REPLACE TABLE domains_stage2 AS SELECT 
    l1.domain, l1.ns, l1.suffix, l2.tld_country, l2.tld_manager, l1.a, l5.isp, l5.isp_country, l1.ptr, l1.cname, 
    l1.mx, l1.mx_domain, l1.mx_suffix, l1.spf, l1.dmarc, l3.mbp, l3.type, l3.country, l3.mx_status_flag,
    l4.phishing, l1.www, l1.www_ptr, l1.www_cname, l1.mail_a, l1.mail_mx, l1.mail_mx_domain, 
    l1.mail_mx_suffix, l1.mail_spf, l1.mail_dmarc, l1.mail_ptr, l1.refresh_date 
    FROM domains_stage1 l1 LEFT JOIN tld_lookup l2 ON split_part(l1.suffix,'.',2) = l2.tld1 
    LEFT JOIN mx_status l3 on l1.mx_domain = l3.mx_domain
    LEFT JOIN phishing l4 on l1.domain = l4.domain
    LEFT JOIN asn_ip4 l5 on l1.ip_int between l5.start_int and l5.end_int
    ;"""

flag = """UPDATE domains_stage2 SET spf_flag = CASE
    WHEN spf = 'v=spf1 -all' THEN  1
    WHEN spf = 'None' THEN NULL
    ELSE 0
    END;"""

files = glob.glob(directory2 + "*refresh.parquet")
for file in files:
    file2 = file.split("\\")[1]
    data = f"INSERT INTO domains_stage1 SELECT * FROM '{directory2 + file2}';"
    output = directory2 + file2.split("_")[0] + "_processed.parquet"
    output_copy = "COPY domains_stage2 TO '{}' (FORMAT PARQUET)".format(output)
    print(file2 + " start: ", timeit.default_timer())
    con = duckdb.connect(directory + db)
    con.sql("""DROP TABLE IF EXISTS domains_stage1;""")

    con.sql(stage1)

    con.sql(data)
    # con.sql("SELECT COUNT(*) FROM domains_stage1;")
    print("domains_stage1 done")

    con.sql(query)

    con.sql("ALTER TABLE domains_stage2 ADD COLUMN spf_flag BOOLEAN;")
    con.sql(flag)
    con.execute(output_copy)
    print(file + " end: ", timeit.default_timer())
