from  unittest import TestCase, main
from DocumentsProcessor import DocumentsProcessor
from dotenv import load_dotenv
import os

class TestProcessor(TestCase):

    def setUp(self):
        # Create a ProcessorImpl instance for testing
        load_dotenv()
        DBUSERNAME = os.getenv("DBUSERNAME")

        PASSWORD = os.getenv("PASSWORD")
        LOCALHOST = os.getenv("LOCALHOST")
        DATABASE = os.getenv("DATABASE")
        databse_url = "postgresql://" + DBUSERNAME + ":" + PASSWORD + "@localhost:" + LOCALHOST + "/" + DATABASE
        print(databse_url)
        self.processor = DocumentsProcessor(databse_url)

    # def tearDown(self):
    #     # Clean up resources after each test
    #     self.processor.close()

    def test_insert_new_document(self):
        url = "10"
        pub_date = 10
        fetch_time = 7
        text = "Simple text content"
        first_fetch_time = 7

        self.processor.process_document(url=url, pub_date=pub_date, fetch_time=fetch_time, text=text)
        result = self.processor.get(url)

        self.assertIsNotNone(result)
        self.assertEqual(result.url, "10")
        self.assertEqual(result.text, "Simple text content")

    def test_update_existing_document(self):
        url = "10"
        pub_date = 15
        fetch_time = 8
        text = "Initial text content"
        self.processor.process_document(url, pub_date, fetch_time, text)

        # Update the document with a newer message

        result = self.processor.process_document(url, pub_date, fetch_time, text)
        self.assertIsNotNone(result)
        self.assertEqual(result.first_fetch_time, 7)  # Check if pub_date remains the same
        self.assertEqual(result.text, "Initial text content")  # Check if text is updated
        self.assertEqual(result.pub_date, 10)

    # def test_ignore_duplicate_message(self):
    #     message = Message(
    #         url="http://example.com/document2",
    #         pub_date=20230101000000,
    #         fetch_time=20230101010000,
    #         text="Sample text content",
    #         first_fetch_time=20230101010000
    #     )
    #     result1 = self.processor.process(message)
    #     result2 = self.processor.process(message)  # Inserting the same message again

    #     self.assertIsNotNone(result1)
    #     self.assertIsNone(result2)  # Should return None for duplicate message

if __name__ == '__main__':
    main()
