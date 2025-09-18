import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from database.models import model_base
from typing import Generator
from sqlalchemy.orm import Session
from contextlib import contextmanager

load_dotenv()

class Connection:
    def __init__(
        self,
        db_hostname: str = None,
        db_port: str = None,
        db_database: str = None,
        db_username: str = None,
        db_password: str = None,
        database_url: str = None
    ):
        if database_url:
            self.database_url = database_url
        else:
            self.db_hostname = db_hostname or os.getenv("DB_HOSTNAME")
            self.db_port = db_port or os.getenv("DB_PORT")
            self.db_database = db_database or os.getenv("DB_DATABASE")
            self.db_username = db_username or os.getenv("DB_USERNAME")
            self.db_password = db_password or os.getenv("DB_PASSWORD")

            self.database_url = f"mssql+pyodbc://{self.db_username}:{self.db_password}@{self.db_hostname}:{self.db_port}/{self.db_database}?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"
        
        print(f"Using database URL: {self.database_url}")

        self.engine = create_engine(
            self.database_url,
            echo=False,  # log
            pool_pre_ping=True,
            pool_recycle=300,
        )

        self.SessionLocal = sessionmaker(
            autocommit=False, autoflush=False, bind=self.engine
        )

    def create_tables(self) -> None:
        model_base.metadata.create_all(bind=self.engine)

    def open_session(self) -> Session:
        return self.SessionLocal()
    
    @contextmanager
    def get_session(self) -> Generator[Session, None, None]:
        session = self.open_session()
        try:
            yield session
        finally:
            session.close()

    def get_session_fastapi(self) -> Generator[Session, None, None]:
        session = self.open_session()
        try:
            yield session
        finally:
            session.close()