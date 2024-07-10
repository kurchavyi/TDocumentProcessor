from sqlalchemy import create_engine, Column, Integer, String, inspect, Numeric
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
import os

Base = declarative_base()


class TDocuments(Base):
    __tablename__ = 'text_documents'
    url = Column(String, primary_key=True)
    pub_date = Column(Numeric(20, 0), nullable=False)
    fetch_time = Column(Numeric(20, 0), nullable=False)
    text = Column(String, nullable=False)
    first_fetch_time = Column(Numeric(20, 0), nullable=False)


class DocumentsProcessor:

    def __init__(self, DATABASE_URL) -> None:
        self.engine = create_engine(DATABASE_URL)
        Base.metadata.create_all(self.engine)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()

    def process_document(self, url, pub_date, fetch_time, text) -> None:
        """
            process document
        """
        document = self.get(url=url)
        if document:
            if fetch_time < document.first_fetch_time:
                document.first_fetch_time = fetch_time
                document.pub_date = pub_date
            if fetch_time > document.fetch_time:
                document.fetch_time = fetch_time
                document.text = text
        else:
            self.__insert(url, pub_date, fetch_time, text, fetch_time)
        return document


    def __insert(self, url, pub_date, fetch_time, text, first_fetch_time) -> None:
        """
            Add document in database.
        """
        new_document = TDocuments(
            url=url,
            pub_date=pub_date,
            fetch_time=fetch_time,
            text=text,
            first_fetch_time=first_fetch_time
        )
        self.session.add(new_document)
        self.session.commit()
        return new_document


    def __update(self, document, url, pub_date, fetch_time, text, first_fetch_time) -> None:
        """
            Update document in database.
        """
        # # переписать это через поиск по primary key
        # document = self.session.query(TDocuments).filter_by(url=url).first()
        document.pub_date = pub_date
        document.fetch_time = fetch_time
        document.text = text
        document.first_fetch_time = first_fetch_time
        self.session.commit()

    def get(self, url):
        """
            Get document from database by given url.
            If document not exist return None.
            If document exist return document.
        """
        # переписать это через поиск по primary key
        document = self.session.query(TDocuments).filter_by(url=url).first()
        return document
