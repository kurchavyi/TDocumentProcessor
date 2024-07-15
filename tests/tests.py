from unittest import TestCase, main

from processor.documents_processor import DocumentsProcessor
from models import TDocuments


class TestProcessor(TestCase):

    def setUp(self):
        self.processor = DocumentsProcessor()

    def test_insert_new_document(self):
        document = TDocuments(
            url="4", pub_date=10, fetch_time=7, text="Simple text content"
        )

        self.processor.process_document(document.to_dict())
        result = self.processor.get("4")

        self.assertIsNotNone(result)
        self.assertEqual(result.url, "4")
        self.assertEqual(result.text, "Simple text content")

    def test_update_existing_document(self):
        text = "Initial text content"
        document = TDocuments(
            url="4", pub_date=15, fetch_time=8, text="Initial text content"
        )
        result = self.processor.process_document(document.to_dict())
        self.assertIsNotNone(result)
        self.assertEqual(result.first_fetch_time, 7)
        self.assertEqual(result.text, "Initial text content")
        self.assertEqual(result.pub_date, 10)


if __name__ == "__main__":
    main()
