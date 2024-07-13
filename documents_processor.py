from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker

from models import TDocuments
from database_creator import get_database_path


class DocumentsProcessor:
    def __init__(self) -> None:
        Base = declarative_base()
        self.engine = create_engine(get_database_path())
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
            self.session.commit()
        else:
            document = TDocuments(
                url=url,
                pub_date=pub_date,
                fetch_time=fetch_time,
                text=text,
                first_fetch_time=fetch_time
        )
            self.__insert(document)
        return document


    def __insert(self, new_document) -> None:
        """
            Add document in database.
        """
        self.session.add(new_document)
        self.session.commit()
        return new_document


    def __update(self, document, url, pub_date, fetch_time, text, first_fetch_time) -> None:
        """
            Update document in database.
        """
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
        return self.session.get(TDocuments, url)
