import sqlalchemy as sa
import sqlalchemy.orm as orm
import sys
import os
import argparse

# Help function gives the -h help message
parser=argparse.ArgumentParser(
    description="""Script used to Create Database """,
    epilog="""Creates new database called [name] with the following tables:
    \n table1
    \n table2
    """)
parser.add_argument('-d', metavar='name', type=str, help='Enter the name of the database, make sure to end on .db', required = True)
args=parser.parse_args()

# Try to get the name from the sys.argv
# Try except to give message if more than 1 or 0 arguments are given
try: 
    name = sys.argv[1]
except:
    raise ValueError("Error: Please enter two arguments.")

# Check whether database already exists
directory = os.listdir(os.getcwd())
if name in directory:
    raise Exception('A database with the same name already exists. Please either specify another name or delete the existing database first.')


# Connect with the engine, create the database and add the tables
Base = orm.declarative_base()
try:
    engine = sa.create_engine(url= f"sqlite:///{name}",echo=False)

    metadata = sa.MetaData()

    messages = sa.Table('Messages', metadata,
    sa.Column('message_id', sa.Integer(), primary_key = True, autoincrement = True),
    sa.Column('time', sa.Integer()),
    sa.Column('attribute', sa.String(160)),
    sa.Column('value', sa.Integer()),
    sa.Column('device_id', sa.Integer()),
    sa.Column('source_id', sa.Integer())
    )

    unitcap = sa.Table('UnitCap', metadata,
    sa.Column('attribute', sa.Integer()),
    sa.Column('capability', sa.String(160), nullable=False),
    sa.Column('unit', sa.String(160), default=False),
    )

    devices = sa.Table('Devices', metadata,
    sa.Column('device_id', sa.String(160), nullable=False),
    sa.Column('label', sa.String(160), nullable=False),
    sa.Column('location', sa.String(160), default=False),
    )

    sources = sa.Table('Sources', metadata,
    sa.Column('source_id', sa.Integer(), primary_key = True, autoincrement = True),
    sa.Column('file_name', sa.String(160)),
    sa.Column('type_file', sa.String(160))
    )

    consumption = sa.Table('Consumption', metadata,
    sa.Column("consumption_id", sa.Integer(),primary_key=True,autoincrement=True),
    sa.Column('time', sa.Integer()),
    sa.Column("type_time", sa.String(160)),
    sa.Column('type', sa.String(160)),
    sa.Column('value', sa.Integer()),
    sa.Column('source_id', sa.Integer())
    )

    metadata.create_all(engine) 
except:
    # Not sure yet if it fails if no .db name is given
    # This seems to cause an error at a later stage
    # Maybe if statement better to check
    raise ValueError("Error: Name database incorrect, please enter .db file")

            