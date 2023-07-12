import sqlalchemy as sa
import sqlalchemy.orm as orm
import pandas as pd

Base = orm.declarative_base()                                       
 
### 1. Classes for different tables
###    Will be used later to add or get data

class Messages(Base):
  """ 
  Used to make an element of the message table.
  Consists of: time, attribute, value, device_id, source_id
  """
  __tablename__ = "Messages"
  
  #message_id = sa.Column(sa.Integer,primary_key=True)
  time = sa.Column(sa.String(160))
  attribute = sa.Column(sa.ForeignKey("UnitCap.attribute"), nullable = False)
  value = sa.Column(sa.Integer())
  device_id = sa.Column(sa.ForeignKey('Devices.device_id'), primary_key=True, nullable = False)
  source_id = sa.Column(sa.ForeignKey('Sources.source_id'), nullable = False)


  def __repr__(self):
    return f"Messages(Time= {self.time}, Attribute={self.attribute}, DeviceID={self.device_id}, SourceID={self.device_id})"


class UnitCap(Base):
  """ 
  Used to make an element of the unitcap table.
  Consists of: attribute, capability, unit
  """
  __tablename__ = "UnitCap"
  
  attribute = sa.Column(sa.String(160),primary_key=True)
  capability = sa.Column(sa.String(160))
  unit = sa.Column(sa.String(160))

  # Relationship with the messages table based on attribute key
  messages = orm.relationship("Messages", backref = "UnitCap")

  def __repr__(self):
    return f"UnitCap(Attribute= {self.attribute}, Capability={self.capability}, Unit={self.unit})"
  
class Devices(Base):
  """ 
  Used to make an element of the device table.
  Consists of: device_id, label, location
  """
  __tablename__ = "Devices"
  
  device_id = sa.Column(sa.String(160),primary_key=True)
  label = sa.Column(sa.String(160))
  location = sa.Column(sa.String(160))

  # Relationship with the messages table based on device_id key
  messages = orm.relationship("Messages", backref = "devices")

  def __repr__(self):
    return f"Devices(DeviceID= {self.device_id}, Label={self.label}, Location={self.location})"

class Sources(Base):
  """ 
  Used to make an element of the sources table.
  Consists of: source_id, file_name, type_file
  """
  __tablename__ = "Sources"
  
  source_id = sa.Column(sa.Integer, primary_key=True, autoincrement=True)
  file_name = sa.Column(sa.String(160))
  type_file = sa.Column(sa.String(160))

  # Relationship with the messages table based on source_id key
  messages = orm.relationship("Messages", backref = "Sources")
  # Relationship with the consumption table based on source_id key
  consumption = orm.relationship("Consumption", backref = "Sources")

  def __repr__(self):
    return f"Sources(SourceID= {self.source_id}, File_Name={self.file_name}, Type_File={self.type_file})"

class Consumption(Base):
   """ 
  Used to make an element of the consumption table.
  Consists of: consumption_id, time, type_time, type, value, source_id
  """
   __tablename__ = "Consumption"

   consumption_id = sa.Column(sa.Integer, primary_key = True, autoincrement=True)
   time = sa.Column(sa.String(), primary_key=True)
   type_time = sa.Column(sa.String(160))
   type = sa.Column(sa.String(160))
   value = sa.Column(sa.Integer)
   source_id = sa.Column(sa.ForeignKey('Sources.source_id'), nullable = False)

   def __repr__(self):
    return f"Consumption(ConsumptionID= {self.consumption_id}, Time={self.time}, Type_Time={self.type_time}, Type={self.type}, Value={self.value}, SourceID={self.source_id})"


### 2. Class interpreter
###    Used to add data to database, and get data from database

class home_messages_db:
    def __init__(self, url):
        """ Creates an interface for the home_message_db based on the database url """
        self._engine = sa.create_engine(url=url, echo=False)

    def addNewEntry(self, newEntry):
        """ 
        Add a single row to the database
        Input has to be one of the following classes: messages, unitcap, devices, sources, consumption
        """
        with orm.Session(self._engine) as session:
            session.add(newEntry)
            session.commit()
    
    def delete_entry(self,tablename,idrow,id):
      """delete an entry in a table given its id in the table
      """
      delete_query=f"DELETE FROM {tablename} WHERE {idrow}={id}"
      try :
        sql_query = sa.text(delete_query)
      except Exception as e : 
        print("Suppresion didn't succeed :", str(e))
      print("entry succesfully deleted")

    def insert_df(self, df, tablename, if_exists='append', index = False):
      """ Adds df to the database """
      df.to_sql(tablename, con= self._engine, if_exists=if_exists, index = index)

    def getData(self, table):
        """ Prints all the content of given table """
        with orm.Session(self._engine) as session:
            a = session.query(table).all()
            print(a)

    def query_df(self, sql_str:str):
      """Runs a SQL QUERY given as str and returns the output as a DataFrame"""
      sql_query = sa.text(sql_str)
      df = pd.read_sql(sql_query, con=self._engine)
      return df

    def getData_pd(self,tablename):
          """ Transforms a table in the database to a pandas dataframe
          """
          query="SELECT * FROM {}".format(tablename)
          dataframe=self.query_df(query)
          return dataframe


    def test_relation(self):
        """Gives an example of how content of 2 tables can be linked (used mainly to test relationships)"""
        with orm.Session(self._engine) as session:
            a = session.query(Devices).first().messages
            print(a)
            


