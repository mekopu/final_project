from sqlalchemy import create_engine


class Connector():
    def __init__(self):
        pass

    def connect_mysql(self, user, password, host, db, port):
        engine = create_engine("mysql+mysqlconnector://{}:{}@{}:{}/{}".format(
            user, password, host, port, db
        ))
        return engine
    

    def connect_postgres(self, user, password, host, db, port):
        engine = create_engine("postgresql+psycopg2://{}:{}@{}:{}/{}".format(
            user, password, host, port, db
        ))
        return engine
        