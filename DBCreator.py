from sqlalchemy import create_engine, Column, Integer, String, inspect, Numeric
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
import os


# Database URL
load_dotenv()
DBUSERNAME = os.getenv("DBUSERNAME")

PASSWORD = os.getenv("PASSWORD")
LOCALHOST = os.getenv("LOCALHOST")
DATABASE = os.getenv("DATABASE")
DATABASE_URL = "postgresql://" + DBUSERNAME + ":" + PASSWORD + "@localhost:" + LOCALHOST + "/" + DATABASE

# Create engine
engine = create_engine(DATABASE_URL)

# Define a base class for declarative models
Base = declarative_base()

#Define a model (table)
class TDocuments(Base):
    __tablename__ = 'text_documents'
    url = Column(String, primary_key=True)
    pub_date = Column(Numeric(20, 0), nullable=False)
    fetch_time = Column(Numeric(20, 0), nullable=False)
    text = Column(String, nullable=False)
    first_fetch_time = Column(Numeric(20, 0), nullable=False)

# # Create all tables
Base.metadata.create_all(engine)

# Create a session
Session = sessionmaker(bind=engine)
session = Session()

# Insert data
doc1 = TDocuments(url="1", pub_date=20, fetch_time=25, text='url="1", pub_date=20, fetch_time=25', first_fetch_time=25)
doc2 = TDocuments(url="2", pub_date=30, fetch_time=40, text='url="2", pub_date=30, fetch_time=40', first_fetch_time=30)
doc3 = TDocuments(url="3", pub_date=30, fetch_time=45, text='url="3", pub_date=30, fetch_time=45', first_fetch_time=45)

session.add(doc1)
session.add(doc2)
session.add(doc3)
session.commit()

# Query data
tdocuments = session.query(TDocuments).all()
for tdocument in tdocuments:
    print(f"Url: {tdocument.url}, fetch time: {tdocument.fetch_time}")

# Describe data
inspector = inspect(engine)
table_names = inspector.get_table_names()
print(f"Tables: {table_names}")

columns = inspector.get_columns('text_documents')
for column in columns:
    print(f"Column: {column['name']}, Type: {column['type']}")
