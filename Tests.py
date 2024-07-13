from  unittest import TestCase, main
from documents_processor import DocumentsProcessor

class TestProcessor(TestCase):

    def setUp(self):
        self.processor = DocumentsProcessor()

    def test_insert_new_document(self):
        url = "4"
        pub_date = 10
        fetch_time = 7
        text = "Simple text content"
        first_fetch_time = 7

        self.processor.process_document(url=url, pub_date=pub_date, fetch_time=fetch_time, text=text)
        result = self.processor.get(url)

        self.assertIsNotNone(result)
        self.assertEqual(result.url, "4")
        self.assertEqual(result.text, "Simple text content")

    def test_update_existing_document(self):
        url = "11"
        pub_date = 15
        fetch_time = 8
        text = "Initial text content"
        result = self.processor.process_document(url, pub_date, fetch_time, text)
        self.assertIsNotNone(result)
        self.assertEqual(result.first_fetch_time, 7)
        self.assertEqual(result.text, "Initial text content")
        self.assertEqual(result.pub_date, 10)

if __name__ == '__main__':
    main()
