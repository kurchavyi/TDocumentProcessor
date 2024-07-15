"""
Utility for managing the state of the database, a wrapper over alembic.
"""
import argparse
import logging

from configargparse import ArgumentParser
from sqlalchemy import create_engine

from processor.utils.argparse import clear_environ
from processor.db.models import Base


ENV_VAR_PREFIX = 'PROCESSOR_'

logging.basicConfig(level=logging.DEBUG)


parser = ArgumentParser(
    auto_env_var_prefix=ENV_VAR_PREFIX, allow_abbrev=False,
    formatter_class=argparse.ArgumentDefaultsHelpFormatter
)
group = parser.add_argument_group('PostgreSQL options')
group.add_argument('--pg-url', type=str, default='postgresql://postgres:postgres@localhost:8000/postgres',
                help='URL to use to connect to the database')



def main():
    args = parser.parse_args()
    clear_environ(lambda i: i.startswith(ENV_VAR_PREFIX))
    DATABASE_URL = args.pg_url
    engine = create_engine(DATABASE_URL)
    Base.metadata.create_all(engine)

if __name__ == '__main__':
    main()