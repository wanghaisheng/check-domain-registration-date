from sqlalchemy import create_engine, Column, Integer, String, DateTime
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.engine.reflection import Inspector
from dotenv import load_dotenv
import os

load_dotenv()

# Get Turso connection details from environment variables
TURSO_URL = os.getenv("TURSO_URL")
TURSO_TOKEN = os.getenv("TURSO_TOKEN")

dbUrl = f"sqlite+{TURSO_URL}/?authToken={TURSO_TOKEN}&secure=true"

class DatabaseManager:
    def __init__(self):
        self.engine = self.get_engine()
        self.Session = self.get_session()
        self._init_db()
        self.Base = declarative_base()
        self.Screenshot = self.define_screenshots_models()
        self.Domain =self.define_domain_models()
    def get_engine(self):
        return create_engine(dbUrl, connect_args={'check_same_thread': False}, echo=True)

    def get_session(self):
        return sessionmaker(bind=self.engine)

    def _init_db(self):
        if not self.check_table_exists():
            print("The 'screenshot' table does not exist. Creating now...")
            self.Base.metadata.create_all(self.engine)
            print('screenshot table ok')
        else:
            print("The 'screenshot' table already exists.")

    def define_screenshots_models(self):
        class Screenshot(self.Base):
            __tablename__ = 'screenshots'

            id = Column(Integer, primary_key=True)
            url = Column(String)
            uuid = Column(String)
            
            # Add additional fields if necessary

        return Screenshot


    def define_domain_models(self):

        class Domain(self.Base):
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
    def check_table_exists(self):
        inspector = Inspector.from_engine(self.engine)
        table_names = inspector.get_table_names()
        return 'screenshots' in table_names

    def add_screenshot(self, new_screenshot_data):
        with self.Session() as session:
            screenshot = session.query(self.Screenshot).filter_by(url=new_screenshot_data.url).first()
            if screenshot:
                for attr, value in new_screenshot_data.__dict__.items():
                    if value is not None:
                        setattr(screenshot, attr, value)
            else:
                session.add(new_screenshot_data)
            session.commit()

    # Add similar methods for add_screenshot_list, read_screenshot_by_url, read_screenshot_all
    def add_screenshot_list(self,new_screenshots):
        with self.Session() as session:
            try:
                for new_screenshot in new_screenshots:
                    session.add(new_screenshot)
                    session.commit()    
            except Exception as e:
                print(f"An error occurred: {e}")
            finally:
                session.close()

    def read_screenshot_by_url(self,url):
        with self.Session() as session:
            domain = session.query(self.Screenshot).filter(self.Screenshot.url == url).first()
            session.close()
            return domain       
    def read_screenshot_all(self):
        with self.Session() as session:

            domains = session.query(self.Screenshot).all()
            # for user in users:
                # print(f"Screenshot: {user.name}, Email: {user.email}")

            # Close the session
            session.close()    
            return domains
    def add_domain(self,new_domain_data):
        with self.Session() as session:

            try:
                domain = session.query(self.Domain).filter_by(url=new_domain_data.url).first()
                if domain:
                    # Update existing domain, ignoring attributes with None values
                    for attr, value in new_domain_data.__dict__.items():
                        if value is not None and getattr(domain, attr) != value:
                            setattr(domain, attr, value)

                else:
                    # Add new domain
                    session.add(new_domain_data)
                session.commit()
                print('add domain ok')
            except Exception as e:
                print(f"An error occurred: {e}")
            finally:
                session.close()

    def add_domain_list(self,new_domains):
        with self.Session() as session:
            try:
                for new_domain in new_domains:
                    session.add(new_domain)
                    session.commit()    
            except Exception as e:
                print(f"An error occurred: {e}")
            finally:
                session.close()

    def read_domain_by_url(self,url):
        with self.Session() as session:
            domain = session.query(self.Domain).filter(self.Domain.url == url).first()
            session.close()
            return domain       
    def read_domain_all(self):
        # Query the database
        with self.Session() as session:

            domains = session.query(self.Domain).all()
            # for user in users:
                # print(f"Domain: {user.name}, Email: {user.email}")

            # Close the session
            session.close()    
            return domains
    def read_domain_all_urls(self):
        # Initialize an empty list to store the queried names
        domain_names = []

        # Query the database
        with self.Session() as session:
            # Query only the 'name' column from the Domain table
            query_result = session.query(self.Domain.url).all()
            
            # Extract names from the query result
            domain_names = [name for (name,) in query_result]
            
            # No need to manually close the session when using 'with' statement

        # 'domain_names' contains only the 'name' column values
        return domain_names