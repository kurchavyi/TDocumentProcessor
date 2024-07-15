
import argparse
import logging
import os
from sys import argv

from configargparse import ArgumentParser
from aiomisc.log import LogFormat, basic_config

from processor.utils.argparse import clear_environ, positive_int
from processor.api.documents_handler import DocumentsHandler


ENV_VAR_PREFIX = 'PROCESSOR_'


parser = ArgumentParser(
    auto_env_var_prefix=ENV_VAR_PREFIX, allow_abbrev=False,
    formatter_class=argparse.ArgumentDefaultsHelpFormatter
)

group = parser.add_argument_group('Kafka Options')
group.add_argument('--kafka-port', default=9092, type=positive_int,
                   help='TCP port Kafka server would listen on')
group.add_argument('--input-topic', default='input_documents',
                   help='The input topic from which documents_handler will read information.')
group.add_argument('--output-topic', default='output_documents',
                   help='The input topic in which documents_handler will write information.')

group = parser.add_argument_group('PostgreSQL options')
group.add_argument('--pg-url', default='postgresql://postgres:postgres@localhost:8000/postgres',
                   help='URL to use to connect to the database')

group = parser.add_argument_group('Logging options')
group.add_argument('--log-level', default='info',
                   choices=('debug', 'info', 'warning', 'error', 'fatal'))
group.add_argument('--log-format', choices=LogFormat.choices(),
                   default='color')


def main():
    args = parser.parse_args()

    clear_environ(lambda i: i.startswith(ENV_VAR_PREFIX))
    basic_config(args.log_level, args.log_format, buffered=True)


    handler = DocumentsHandler(args)
    handler.run()


if __name__ == '__main__':
    main()