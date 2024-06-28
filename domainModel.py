from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.orm import sessionmaker, declarative_base
from db import create_turso_engine
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Get Turso connection details from environment variables
TURSO_URL = os.getenv("TURSO_URL")
TURSO_TOKEN = os.getenv("TURSO_TOKEN")

# Create the engine
engine = create_turso_engine(TURSO_URL, TURSO_TOKEN)

# Create a session
Session = sessionmaker(bind=engine)
session = Session()

# Define your models
Base = declarative_base()

class Domain(Base):
    __tablename__ = 'domains'

    id = Column(Integer, primary_key=True)
    domain = Column(String)
    title = Column(String)
    des = Column(String)
    bornat = Column(String)
    indexat = Column(String)
    indexdata = Column(String)

# Create the tables
Base.metadata.create_all(engine)

# Use the session to interact with the database
new_user = Domain(name='John Doe', email='john@example.com')
session.add(new_user)
session.commit()

# Query the database
users = session.query(Domain).all()
for user in users:
    print(f"Domain: {user.name}, Email: {user.email}")

# Close the session
session.close()