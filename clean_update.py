import duckdb
import timeit
import glob

directory = "/root/"
directory2 = "/root/"
db = "domains_all_postgres.duckdb"

con = duckdb.connect(directory + db)
print("Connected to DuckDB")

stage1 = """CREATE TABLE IF NOT EXISTS domains_stage1 (
    domain VARCHAR,
    ns VARCHAR,
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
    l1.domain, l1.ns, l1.suffix, l2.tld_country, l2.tld_manager, l1.a, l8.isp, l8.isp_country, l1.ptr, l1.cname,
    l1.mx, l1.mx_domain, l1.mx_suffix, l1.spf, l1.dmarc, l3.mbp, l3.type, l3.country, l6.wm_country, l6.wm_group,
    l3.mx_status_flag, COALESCE(l3.is_mailable,0) AS is_mailable, COALESCE(l3.is_disposable,0) AS is_disposable,
    COALESCE(l3.is_known_mbp,0) AS is_known_mbp, COALESCE(l4.is_phishing,0) AS is_phishing,
    COALESCE(l5.is_malware,0) AS is_malware, COALESCE(l6.is_webmail,0) AS is_webmail,
    l1.www, l1.www_ptr, l1.www_cname, l1.mail_a, l1.mail_mx, l1.mail_mx_domain,
    l1.mail_mx_suffix, l1.mail_spf, l1.mail_dmarc, l1.mail_ptr, l7.top_domain_rank, l9.create_date, l1.refresh_date
    FROM domains_stage1 l1
    LEFT JOIN tld_lookup l2 ON l1.suffix = l2.tld1
    LEFT JOIN mx_status l3 on l1.mx_domain = l3.mx_domain
    LEFT JOIN phishing_domains l4 on l1.domain = l4.domain
    LEFT JOIN malware_domains l5 on l1.domain = l5.domain
    LEFT JOIN webmail l6 on l1.domain = l6.domain
    LEFT JOIN top_1m l7 on l1.domain = l7.domain
    LEFT JOIN asn_ip4 l8 on l1.ip_int between l8.start_int and l8.end_int
    LEFT JOIN create_date l9 ON l1.domain = l9.domain
    ;"""

spf_block = """UPDATE domains_stage2 SET is_spf_block = CASE
    WHEN spf = 'v=spf1 -all' THEN  1
    WHEN spf = 'None' THEN NULL
    ELSE 0
    END;"""

parked = """UPDATE domains_stage2 SET is_parked = CASE
	    WHEN mx_status_flag = 'Parked Domain' THEN 1
            WHEN www_cname LIKE '%parking%' THEN 1
            WHEN ptr LIKE '%parked%' THEN 1
            WHEN ptr LIKE '%parking%' THEN 1
            WHEN ns LIKE '%park%' THEN 1
            WHEN ns LIKE '%sedo%' THEN 1
            WHEN ns LIKE '%bodis%' THEN 1
            ELSE 0
            END;""" 

age = """UPDATE domains_stage2 SET is_new_domain = CASE
         WHEN datediff('days', create_date, today()) < 31 THEN 1
         ELSE 0
         END;"""

suffix_flag = """UPDATE domains_stage2
                 SET mx_status_flag = mx_suffix.mx_status_flag
		     FROM mx_suffix
                     WHERE mx_suffix = mx_suffix.suffix;"""
suffix_mbp = """UPDATE domains_stage2
                SET mbp = mx_suffix.mbp
                FROM mx_suffix
                WHERE mx_suffix = mx_suffix.suffix;"""

suffix_type = """UPDATE  domains_stage2
                    SET type = mx_suffix.type
                    FROM mx_suffix
                    WHERE mx_suffix = mx_suffix.suffix;"""

suffix_country = """UPDATE domains_stage2
                    SET country = mx_suffix.country
                    from mx_suffix
                    WHERE mx_suffix = mx_suffix.suffix;"""

mailable = """UPDATE domains_stage2 SET is_mailable = CASE
              WHEN mx_domain IS NOT NULL THEN 1
              WHEN mail_mx_domain IS NOT NULL THEN 1
              WHEN ptr IS NOT NULL THEN 1
              ELSE 0
              END;"""

overall_flag = """UPDATE domains_stage2 SET decision_flag = CASE
                  WHEN mx_status_flag IN ('Spam Trap', 'Disposable', 'Dormant', 'Parked Domain', 'Phishing', 'Suspicious', 'Testing', 'Typo Domain')  THEN 0
                  WHEN is_spf_block = 1 THEN 0
                  WHEN is_parked = 1 THEN 0
                  WHEN is_malware = 1 THEN 0
                  WHEN is_phishing = 1 THEN 0
                  WHEN is_disposable = 1 THEN 0
                  ELSE 1
                  END;"""
'''
tld_lookup = """
             ALTER TABLE domains_stage3 ADD COLUMN matched_tld TEXT;
             ALTER TABLE domains_stage3 ADD COLUMN tld_country TEXT;
             ALTER TABLE domains_stage3 ADD COLUMN tld_manager TEXT;

             WITH ranked_matches AS (
                SELECT t1.*, lt.*,
                  ROW_NUMBER() OVER (PARTITION BY t1.suffix ORDER BY LENGTH(lt.tld1) DESC) AS rank
                FROM domains_stage3 t1
             LEFT JOIN tld_lookup lt
             ON t1.suffix ~ ('(\\.|^)' || lt.tld1 || '$')
             )
             UPDATE domains_stage3 t1
             SET matched_tld = rm.tld1, tld_country = rm.tld_country, tld_manager = rm.tld_manager
             FROM ranked_matches rm
             WHERE t1.suffix = rm.tld1
             AND rm.rank = 1;"""



tld_lookup = """CREATE OR REPLACE TABLE domains_stage4 AS
                 WITH ranked_matches AS (
                  SELECT t1.*, lt.tld1, lt.tld_country, lt.tld_manager,
                    ROW_NUMBER() OVER (PARTITION BY t1.suffix ORDER BY LENGTH(lt.tld1) DESC) AS rank
                  FROM domains_stage3 t1
                  LEFT JOIN tld_lookup lt
                  ON t1.suffix ~ ('(\\.|^)' || lt.tld1 || '$')
                )
                SELECT t1.domain, t1.top_domain_rank, t1.ns, t1.suffix, rm.tld1, rm.tld_country, rm.tld_manager, 
                       t1.a, t1.isp, t1.isp_country, t1.ptr, t1.cname, t1.mx, t1.mx_domain, t1.mx_suffix, 
                       t1.spf, t1.dmarc, t1.mbp, t1.type, t1.country, t1.mx_status_flag, t1.wm_country, 
                       t1.wm_group, t1.www, t1.www_ptr, t1.www_cname, t1.mail_a, t1.mail_mx, t1.mail_mx_domain, 
                       t1.mail_mx_suffix, t1.mail_spf, t1.mail_dmarc, t1.mail_ptr, t1.is_mailable, 
                       t1.is_known_mbp, t1.is_webmail, t1.is_disposable, t1.is_phishing, t1.is_malware,
                       t1.is_spf_block, t1.is_parked, t1.is_new_domain, t1.decision_flag, t1.create_date, 
                       t1.refresh_date
                FROM domains_stage3 t1
                LEFT JOIN ranked_matches rm
                ON t1.suffix = rm.tld1
                AND rm.rank = 1"""
'''

uniques = """CREATE OR REPLACE TABLE domains_stage3 AS SELECT
             DISTINCT ON(domain) * FROM domains_stage2;"""

file = directory + 'domains_all.parquet'
data = f"INSERT INTO domains_stage1 SELECT * FROM '{file}';"
output = file.split("_")[0] + "_processed.parquet"
output_copy = "COPY domains_stage3 TO '{}' (FORMAT PARQUET)".format(output)
print(file + " start: ", timeit.default_timer())
con = duckdb.connect(directory + db)
con.sql("""DROP TABLE IF EXISTS domains_stage1;""")

con.sql(stage1)

con.sql(data)

print("domains_stage1 done")

con.sql(query)

con.sql("ALTER TABLE domains_stage2 ADD COLUMN is_spf_block BOOLEAN DEFAULT 0;")
con.sql("ALTER TABLE domains_stage2 ADD COLUMN is_parked BOOLEAN DEFAULT 0;")
con.sql("ALTER TABLE domains_stage2 ADD COLUMN is_new_domain BOOLEAN DEFAULT 0;")
con.sql("ALTER TABLE domains_stage2 ADD COLUMN decision_flag BOOLEAN DEFAULT 1;")
con.sql(spf_block)
con.sql(parked)
con.sql(age)
con.sql(suffix_flag)
con.sql(suffix_mbp)
con.sql(suffix_type)
con.sql(suffix_country)
con.sql(suffix_type)
con.sql(mailable)
con.sql(overall_flag)
con.sql(uniques)
con.execute(output_copy)
print(file + " end: ", timeit.default_timer())
