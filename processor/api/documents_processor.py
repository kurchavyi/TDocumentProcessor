from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker

from processor.db.models import TDocuments


class DocumentsProcessor:
    """
    Handle the insertion and updating of documents in a database using SQLAlchemy.
    """
    def __init__(self, pg_url) -> None:
        Base = declarative_base()
        self.engine = create_engine(pg_url)
        Base.metadata.create_all(self.engine)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()

    def process_document(self, dict_document: dict) -> TDocuments:
        """
        Insert document into the database if it does not exist.
        Updating document if it exists.
        """
        new_document = TDocuments.from_dict(dict_document)
        old_document = self.get(url=new_document.url)
        if old_document:
            result_document = self.__update(
                old_document=old_document, new_document=new_document
            )
        else:
            result_document = self.__insert(new_document)
        return result_document

    def __insert(self, new_document: TDocuments) -> TDocuments:
        """
        Insert a new document into the database.
        """
        new_document.first_fetch_time = new_document.fetch_time
        self.session.add(new_document)
        self.session.commit()
        return new_document

    def __update(
        self, old_document: TDocuments, new_document: TDocuments
    ) -> TDocuments:
        """
        Update an existing document in the database.
        """
        if new_document.fetch_time < old_document.first_fetch_time:
            old_document.first_fetch_time = new_document.fetch_time
            old_document.pub_date = new_document.pub_date
        if new_document.fetch_time > old_document.fetch_time:
            old_document.fetch_time = new_document.fetch_time
            old_document.text = new_document.text
        self.session.commit()
        return old_document

    def get(self, url: str) -> TDocuments:
        """
        Retrieve a document from the database by its URL.
        If document does not exist return None.
        """
        return self.session.get(TDocuments, url)
