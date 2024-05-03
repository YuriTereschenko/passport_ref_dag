import vertica_python


class VerticaConnect:
    def __init__(self, host: str, port: str, db_name: str, user: str, pw: str) -> None:
        self.conn_data = {
            'host': host,
            'port': port,
            'database': db_name,
            'user': user,
            'password': pw
        }

    def connection(self):
        return vertica_python.connect(**self.conn_data)
