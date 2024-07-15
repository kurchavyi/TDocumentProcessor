from sqlalchemy import MetaData, Column, Table, Numeric, String, 

convention = {
    'all_column_names': lambda constraint, table: '_'.join([
        column.name for column in constraint.columns.values()
    ]),
    'ix': 'ix__%(table_name)s__%(all_column_names)s',
    'uq': 'uq__%(table_name)s__%(all_column_names)s',
    'ck': 'ck__%(table_name)s__%(constraint_name)s',
    'fk': 'fk__%(table_name)s__%(all_column_names)s__%(referred_table_name)s',
    'pk': 'pk__%(table_name)s'
}

metadata = MetaData(naming_convention=convention)

documents_table = Table(
    'documents_table',
    metadata,
    Column('url', String, primary_key=True),
    Column('pub_date', Numeric(20, 0), nullable=False),
    Column('fetch_time', Numeric(20, 0), nullable=False),
    Column('text', String, nullable=False),
    Column('first_fetch_time', Numeric(20, 0), nullable=False)
)
