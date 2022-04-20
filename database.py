import pyodbc
from sqlalchemy import create_engine
import urllib

def connection():
    connection_string = 'Driver={ODBC Driver 17 for SQL Server};Server=tcp:ginspiserver.database.windows.net,1433;Database=ginspi_db;Uid=ginspi;Pwd=2241023544aA@;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'

    params = urllib.parse.quote_plus(connection_string)
    conn_str = 'mssql+pyodbc:///?odbc_connect={}'.format(params)
    engine_azure = create_engine(conn_str,echo=True)
    return engine_azure