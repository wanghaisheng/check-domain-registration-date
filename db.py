from sqlalchemy.dialects.sqlite.pysqlite import SQLiteDialect_pysqlite
from sqlalchemy.engine import default
from libsql_client import Client

class TursoDialect(SQLiteDialect_pysqlite):
    name = "turso"

    @classmethod
    def dbapi(cls):
        return TursoDBAPI()

class TursoDBAPI:
    def __init__(self):
        self.client = None

    def connect(self, url, token):
        self.client = Client(url=url, auth_token=token)
        return TursoConnection(self.client)

class TursoConnection:
    def __init__(self, client):
        self.client = client

    def cursor(self):
        return TursoCursor(self.client)

    def close(self):
        pass

class TursoCursor:
    def __init__(self, client):
        self.client = client
        self.results = None

    def execute(self, operation, parameters=None):
        if parameters:
            self.results = self.client.execute(operation, parameters)
        else:
            self.results = self.client.execute(operation)
        return self

    def fetchall(self):
        return self.results.rows

    def close(self):
        pass

class TursoEngine(default.DefaultEngine):
    name = "turso"
    dialect = TursoDialect

def create_turso_engine(url, token):
    return TursoEngine.create(url=url, token=token)