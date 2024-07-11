from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
from models import TDocuments
import os

def get_database_path():
    load_dotenv()
    DBUSERNAME = os.getenv("DBUSERNAME")

    PASSWORD = os.getenv("PASSWORD")
    LOCALHOST = os.getenv("LOCALHOST")
    DATABASE = os.getenv("DATABASE")
    DATABASE_URL = "postgresql://" + DBUSERNAME + ":" + PASSWORD + "@localhost:" + LOCALHOST + "/" + DATABASE
    return DATABASE_URL

if __name__ == "__main__":
    DATABASE_URL = get_database_path()

    engine = create_engine(DATABASE_URL)

    Base = declarative_base()

    Base.metadata.create_all(engine)

    Session = sessionmaker(bind=engine)
    session = Session()

    doc1 = TDocuments(url="1", pub_date=20, fetch_time=25, text='url="1", pub_date=20, fetch_time=25', first_fetch_time=25)
    doc2 = TDocuments(url="2", pub_date=30, fetch_time=40, text='url="2", pub_date=30, fetch_time=40', first_fetch_time=30)
    doc3 = TDocuments(url="3", pub_date=30, fetch_time=45, text='url="3", pub_date=30, fetch_time=45', first_fetch_time=45)

    session.add(doc1)
    session.add(doc2)
    session.add(doc3)
    session.commit()

    tdocuments = session.query(TDocuments).all()
    for tdocument in tdocuments:
        print(f"Url: {tdocument.url}, fetch time: {tdocument.fetch_time}")

    inspector = inspect(engine)
    table_names = inspector.get_table_names()
    print(f"Tables: {table_names}")

    columns = inspector.get_columns('text_documents')
    for column in columns:
        print(f"Column: {column['name']}, Type: {column['type']}")
