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

    def process_document(self, dict_document) -> None:
        """
            process document
        """
        new_document = TDocuments.from_dict(dict_document)
        old_document = self.get(url=new_document.url)
        if old_document:
            result_document = self.__update(old_document=old_document, new_document=new_document)
        else:
            result_document = self.__insert(new_document)
        return result_document


    def __insert(self, new_document: TDocuments) -> TDocuments:
        """
            Add document in database.
        """
        new_document.first_fetch_time = new_document.fetch_time
        self.session.add(new_document)
        self.session.commit()
        return new_document


    def __update(self, old_document : TDocuments, new_document: TDocuments) -> TDocuments:
        """
            Update document in database.
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
            Get document from database by given url.
            If document not exist return None.
            If document exist return document.
        """
        return self.session.get(TDocuments, url)
