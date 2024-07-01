import asyncio
import os
from sqlalchemy import create_engine, Column, Integer, String, DateTime, select
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.engine.reflection import Inspector
from dotenv import load_dotenv

load_dotenv()

# Get Turso connection details from environment variables
TURSO_URL = os.getenv("TURSO_URL")
TURSO_TOKEN = os.getenv("TURSO_TOKEN")

dbUrl = f"sqlite+{TURSO_URL}/?authToken={TURSO_TOKEN}&secure=true"

Base = declarative_base()

class DatabaseManager:
    def __init__(self):
        self.engine = create_async_engine(dbUrl, echo=True, future=True)
        self.Session = sessionmaker(bind=self.engine, class_=AsyncSession, expire_on_commit=False)
        self._init_db()
        self.Screenshot = self.define_screenshots_models()
        self.Domain = self.define_domain_models()

    def _init_db(self):
        if not self.check_table_exists():
            print("The 'screenshot' table does not exist. Creating now...")
            asyncio.run(self.Base.metadata.create_all(self.engine))
            print('screenshot table ok')
        else:
            print("The 'screenshot' table already exists.")

    def define_screenshots_models(self):
        class Screenshot(Base):
            __tablename__ = 'screenshots'

            id = Column(Integer, primary_key=True)
            url = Column(String)
            uuid = Column(String)

            # Add additional fields if necessary

        return Screenshot

    def define_domain_models(self):
        class Domain(Base):
            __tablename__ = 'domains'

            id = Column(Integer, primary_key=True)
            url = Column(String)
            tld = Column(String)
            title = Column(String)
            des = Column(String)
            bornat = Column(String)
            indexat = Column(String)
            indexdata = Column(String)
            uuid = Column(String)

        return Domain

    async def get_session(self):
        async with self.Session() as session:
            yield session

    async def add_screenshot(self, new_screenshot_data):
        async with self.get_session() as session:
            try:
                screenshot = await session.execute(select(self.Screenshot).filter_by(url=new_screenshot_data.url).first())
                if screenshot:
                    for attr, value in new_screenshot_data.__dict__.items():
                        if value is not None:
                            setattr(screenshot, attr, value)
                else:
                    session.add(new_screenshot_data)
                await session.commit()
            except asyncio.TimeoutError:
                print(f"Timeout occurred during add_screenshot after {timeout} seconds.")
            except Exception as e:
                print(f"An error occurred during add_screenshot: {e}")

    async def add_screenshot_list(self, new_screenshots):
        async with self.get_session() as session:
            try:
                for new_screenshot in new_screenshots:
                    session.add(new_screenshot)
                await session.commit()
            except asyncio.TimeoutError:
                print(f"Timeout occurred during add_screenshot_list after {timeout} seconds.")
            except Exception as e:
                print(f"An error occurred during add_screenshot_list: {e}")

    async def read_screenshot_by_url(self, url):
        async with self.get_session() as session:
            try:
                screenshot = await session.execute(select(self.Screenshot).filter_by(url=url).first())
                return screenshot.scalars().first() if screenshot else None
            except asyncio.TimeoutError:
                print(f"Timeout occurred during read_screenshot_by_url after {timeout} seconds.")
                return None
            except Exception as e:
                print(f"An error occurred during read_screenshot_by_url: {e}")
                return None

    async def read_screenshot_all(self):
        async with self.get_session() as session:
            try:
                screenshots = await session.execute(select(self.Screenshot))
                return screenshots.scalars().all()
            except asyncio.TimeoutError:
                print(f"Timeout occurred during read_screenshot_all after {timeout} seconds.")
                return []
            except Exception as e:
                print(f"An error occurred during read_screenshot_all: {e}")
                return []

    async def add_domain(self, new_domain_data):
        async with self.get_session() as session:
            try:
                domain = await session.execute(select(self.Domain).filter_by(url=new_domain_data.url).first())
                if domain:
                    for attr, value in new_domain_data.__dict__.items():
                        if value is not None and getattr(domain, attr) != value:
                            setattr(domain, attr, value)
                else:
                    session.add(new_domain_data)
                await session.commit()
                print('add domain ok')
            except asyncio.TimeoutError:
                print(f"Timeout occurred during add_domain after {timeout} seconds.")
            except Exception as e:
                print(f"An error occurred during add_domain: {e}")

    async def add_domain_list(self, new_domains):
        async with self.get_session() as session:
            try:
                for new_domain in new_domains:
                    session.add(new_domain)
                await session.commit()
            except asyncio.TimeoutError:
                print(f"Timeout occurred during add_domain_list after {timeout} seconds.")
            except Exception as e:
                print(f"An error occurred during add_domain_list: {e}")

    async def read_domain_by_url(self, url):
        async with self.get_session() as session:
            try:
                domain = await session.execute(select(self.Domain).filter_by(url=url).first())
                return domain.scalars().first() if domain else None
            except asyncio.TimeoutError:
                print(f"Timeout occurred during read_domain_by_url after {timeout} seconds.")
                return None
            except Exception as e:
                print(f"An error occurred during read_domain_by_url: {e}")
                return None

    async def read_domain_all(self):
        async with self.get_session() as session:
            try:
                domains = await session.execute(select(self.Domain))
                return domains.scalars().all()
            except asyncio.TimeoutError:
                print(f"Timeout occurred during read_domain_all after {timeout} seconds.")
                return []
            except Exception as e:
                print(f"An error occurred during read_domain_all: {e}")
                return []

    async def read_domain_all_urls(self):
        async with self.get_session() as session:
            try:
                query_result = await session.execute(select(self.Domain.url))
                return [name for (name,) in query_result]
            except asyncio.TimeoutError:
                print(f"Timeout occurred during read_domain_all_urls after {timeout} seconds.")
                return []
            except Exception as e:
                print(f"An error occurred during read_domain_all_urls: {e}")
                return []

    def check_table_exists(self):
        inspector = Inspector.from_engine(self.engine)
        table_names = inspector.get_table_names()
        return 'screenshots' in table_names

