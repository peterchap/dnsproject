from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, DateTime, Boolean
from sqlalchemy.orm import sessionmaker
from fastapi import FastAPI, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from sqlalchemy import select
from typing import List
from pydantic import BaseModel, Field
from datetime import datetime


# Update the Config class inside the DomainDataSchema to use the new key 'from_attributes' instead of 'orm_mode'
class DomainDataSchema(BaseModel):
    # ... existing fields ...

    class Config:
        from_attributes = True


user = "postgres"
password = "1Francis2"
host = "82.208.21.122"
port = "5432"
database = "domains_final"

connection_str = f"postgresql://{user}:{password}@{host}:{port}/{database}"
# SQLAlchemy engine
engine = create_engine(connection_str)
# you can test if the connection is made or not
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()


class DomainData(Base):
    __tablename__ = "domains_all"  # Name of the table in your database

    domain = Column(String, primary_key=True)
    ns = Column(String)
    suffix = Column(String)
    tld_country = Column(String)
    tld_manager = Column(String)
    a = Column(String)
    isp = Column(String)
    isp_country = Column(String)
    ptr = Column(String)
    cname = Column(String)
    mx = Column(String)
    mx_domain = Column(String)
    mx_suffix = Column(String)
    spf = Column(String)
    dmarc = Column(String)
    mbp = Column(String)
    type = Column(String)
    country = Column(String)
    mx_status_flag = Column(String)
    mailable = Column(Boolean)
    disposable = Column(Boolean)
    known_mbp = Column(Boolean)
    phishing = Column(String)
    www = Column(String)
    www_ptr = Column(String)
    www_cname = Column(String)
    mail_a = Column(String)
    mail_mx = Column(String)
    mail_mx_domain = Column(String)
    mail_mx_suffix = Column(String)
    mail_spf = Column(String)
    mail_dmarc = Column(String)
    mail_ptr = Column(String)
    create_date = Column(DateTime())
    refresh_date = Column(DateTime())
    mx_present = Column(Boolean)
    spf_block = Column(Boolean)
    spf_present = Column(Boolean)
    dmarc_present = Column(Boolean)
    age_flag = Column(Boolean)
    parked_flag = Column(Boolean)

    # Add other fields if needed, matching the columns of your database table


# Pydantic Schema
class DomainDataSchema(BaseModel):
    domain: str
    ns: str
    suffix: str
    tld_country: str
    tld_manager: str
    a: str
    isp: str
    isp_country: str
    ptr: str
    cname: str
    mx: str
    mx_domain: str
    mx_suffix: str
    spf: str
    dmarc: str
    mbp: str
    type: str
    country: str
    mx_status_flag: str
    mailable: bool
    disposable: bool
    known_mbp: bool
    phishing: str
    www: str
    www_ptr: str
    www_cname: str
    mail_a: str
    mail_mx: str
    mail_mx_domain: str
    mail_mx_suffix: str
    mail_spf: str
    mail_dmarc: str
    mail_ptr: str
    create_date: datetime
    refresh_date: datetime
    mx_present: bool
    spf_block: bool
    spf_present: bool
    dmarc_present: bool
    age_flag: bool
    parked_flag: bool

    class Config:
        from_attributes = True


# from . import models
Base.metadata.create_all(bind=engine)

app = FastAPI()
# Set up CORS middleware
origins = [
    "http://localhost:3000",  # React app is served from here
    "http://localhost:8000",  # FastAPI server
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Dependency
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@app.get("/data/{domain}")
def read_data(domain: str, db: Session = Depends(get_db)):
    # Replace `YourModel` with your actual model and implement the query logic
    # results = db.query(models.YourModel).filter(models.YourModel.domain == domain).all()
    # Here, just an example response
    db_domain_data = db.query(DomainData).filter(DomainData.domain == domain).first()
    if db_domain_data is None:
        raise HTTPException(status_code=404, detail="Domain not found")
    return db_domain_data
