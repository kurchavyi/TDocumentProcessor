from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, String, Numeric


Base = declarative_base()


class TDocuments(Base):
    __tablename__ = "text_documents"
    url = Column(String, primary_key=True)
    pub_date = Column(Numeric(20, 0), nullable=False)
    fetch_time = Column(Numeric(20, 0), nullable=False)
    text = Column(String, nullable=False)
    first_fetch_time = Column(Numeric(20, 0), nullable=False)

    def to_dict(self):
        return {
            column.name: getattr(self, column.name) for column in self.__table__.columns
        }

    @classmethod
    def from_dict(cls, attr_dict: dict):
        return cls(**attr_dict)
