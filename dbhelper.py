from sqlalchemy import create_engine, Column, Integer, String,DateTime
from sqlalchemy.orm import sessionmaker, declarative_base
# from dbHelpers import create_turso_engine
from dotenv import load_dotenv
from sqlalchemy.engine.reflection import Inspector
import os
load_dotenv()

# Get Turso connection details from environment variables
TURSO_URL = os.getenv("TURSO_URL")
TURSO_TOKEN = os.getenv("TURSO_TOKEN")

dbUrl = f"sqlite+{TURSO_URL}/?authToken={TURSO_TOKEN}&secure=true"

def getEngine():
    # Load environment variables from .env file



    engine = create_engine(dbUrl, connect_args={'check_same_thread': False}, echo=True)
    return engine
def getSession(engine):
    # Create a session
    Session = sessionmaker(bind=engine)
    session = Session()
    return session
# Define your models
Base = declarative_base()

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

def initDB():
# Example usage:
    if not check_table_exists():
        print("The 'domains' table does not exist. Creating now...")
        # Create the table manually or perform other actions
        Base.metadata.create_all(engine)  # Uncomment this line if you decide to create the table
        print('domsin table ok')
    else:
        print("The 'domains' table already exists.")
def add_domain(new_domain_data):
    session = Session()

    session = Session()
    try:
        # Assuming 'url' is the unique identifier for a domain
        domain = session.query(Domain).filter_by(url=new_domain_data['url']).first()
        if domain:
            # Update existing domain
            for key, value in new_domain_data.items():
                if value is not None:
                    setattr(domain, key, value)

        else:
            # Add new domain
            domain = Domain(**new_domain_data)
            session.add(domain)
        session.commit()
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        session.close()

def add_domain_list(new_domains):
    session = Session()
    try:
        for new_domain in new_domains:
            session.add(new_domain)
            session.commit()    
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        session.close()

def read_domain_by_url(url):
    session = Session()
    domain = session.query(Domain).filter(Domain.url == url).first()
    session.close()
    return domain       
def read_domain_all():
    # Query the database
    session = Session()

    domains = session.query(Domain).all()
    # for user in users:
        # print(f"Domain: {user.name}, Email: {user.email}")

    # Close the session
    session.close()    
    return domains

# Create an engine
# print(TURSO_URL)
# print(dbUrl)
engine = create_engine(dbUrl, echo=True)

# Create a sessionmaker bound to the engine
Session = sessionmaker(bind=engine)

# Reflect the Base classes and create a session
inspector = Inspector.from_engine(engine)

# Function to check if the 'domains' table exists
def check_table_exists():
    table_names = inspector.get_table_names()
    return 'domains' in table_names

initDB()