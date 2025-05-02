from sqlalchemy import Column, Integer, String, create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

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

class DatabaseManager:
    def __init__(self, db_url='sqlite:///domains.db'):
        self.engine = create_engine(db_url)
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)

    def add_domain(self, domain: Domain):
        session = self.Session()
        session.add(domain)
        session.commit()
        session.close()

    def read_domain_all(self):
        session = self.Session()
        domains = session.query(Domain).all()
        session.close()
        return domains 