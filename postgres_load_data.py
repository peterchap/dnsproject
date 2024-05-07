import duckdb

conn = duckdb.connect()

conn.sql(
    """ATTACH 'dbname=domains_final user=postgres password=1Francis2 host=82.208.21.122' AS db (TYPE POSTGRES);"""
)
#print(conn.sql("""DESCRIBE db.public.domains_all;"""))

stage1 = """CREATE TABLE IF NOT EXISTS db.public.domains_all (
    domain VARCHAR,
    ns VARCHAR,
    suffix VARCHAR,
    tld_country VARCHAR,
    tld_manager VARCHAR,
    a VARCHAR,
    isp VARCHAR,
    isp_country VARCHAR,
    ptr VARCHAR,
    cname VARCHAR,
    mx VARCHAR,
    mx_domain VARCHAR,
    mx_suffix VARCHAR,
    spf VARCHAR,
    dmarc VARCHAR,
    mbp VARCHAR,
    type VARCHAR,
    country VARCHAR,
    wm_country VARCHAR,
    wm_group VARCHAR,
    mx_status_flag VARCHAR,
    is_mailable BOOLEAN,
    is_disposable BOOLEAN,
    is_known_mbp BOOLEAN,
    is_phishing BOOLEAN,
    is_malware BOOLEAN,
    is_webmail BOOLEAN,
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
    top_domain_rank INTEGER,
    create_date DATETIME,
    refresh_date DATETIME,
    is_spf_block BOOLEAN,
    is_parked BOOLEAN,
    is_new_domain BOOLEAN,
    decision_flag BOOLEAN)"""



data = """ COPY db.public.domains_all_2 FROM 'root/refresh/*processed.parquet';"""


data = """COPY db.public.domains_all FROM '/root/refresh/*processed.parquet';"""
conn.sql("DELETE FROM db.public.domains_all;")
conn.sql(stage1)
print("domains_all done")
print(conn.sql("""DESCRIBE db.public.domains_all;"""))
conn.sql(data)
print("data done")
print(conn.sql("""SELECT COUNT(*) FROM db.public.domains_all;"""))
