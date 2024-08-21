import os

import pandas as pd
from typing import Literal
import pickle as pkl

import sqlalchemy as sa
from sqlalchemy.sql.ddl import DropSchema
from sqlalchemy import MetaData, inspect, create_engine, text
from sqlalchemy.orm import sessionmaker

from typing import Optional, Dict, Any

from models.erorrs import DBConfigError
from models.protcs import QueryConfig, KerberosConfig
from models.utils import Model

try:
    from sqlalchemy.orm import declarative_base
except:
    from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy.schema import CreateSchema

import logging


class DBConfig(Model):
    def __init__(self, delicate: str = 'postgresql', host: str = 'localhost', port: int = 5432,
                 database: Optional[str] = None, username: Optional[str] = None, password: Optional[str] = None,
                 query: Optional[Dict] = None, stream: bool = False, echo: bool = False,
                 kerberos: Optional[Dict] = None, logger=None):

        self._logger = logger if logger else logging.getLogger(__name__)
        # self._encryption_key = Fernet.generate_key()  # TODO: In practice, save this securely and reuse it
        # self._crypto = CryptoHandler(
        #     key=self._encryption_key,
        #     logger=self._logger
        # )
        self._delicate = delicate
        self._host = host
        self._port = port
        self._database = database
        self._username = username
        self._password = password
        self._stream = stream
        self._echo = echo

        self._query = QueryConfig(**query) if query else None
        if self._query:
            self._query.convert_jks_cert(self._username)

        if kerberos:
            self._kerberos = KerberosConfig(**kerberos)
            self._logger.info(
                f"Kerberos Config - \n"
                f"krb5_config: {self._kerberos.krb5_config}, \n"
                f"principal: {self._kerberos.principal}, \n"
                f"keytab_path: {self._kerberos.keytab_path}, \n"
                f"kerberos_service_name: {self._kerberos.kerberos_service_name}\n",
            )
        else:
            self._kerberos = None

    # @property
    # def crypto(self):
    #     """Lazily initialize and return the CryptoHandler."""
    #     if not self._crypto and self._encryption_key:
    #         self._crypto = CryptoHandler(key=self._encryption_key, logger=self._logger)
    #     return self._crypto

    @property
    def kerberos(self):
        return self._kerberos

    @kerberos.setter
    def kerberos(self, kerberos: dict):
        try:
            self._kerberos = KerberosConfig(**kerberos)
        except TypeError as e:
            self._logger.error(f"Incorrect Kerberos configuration: {str(e)}")
            raise ValueError(f"Incorrect Kerberos configuration: {str(e)}")

    @property
    def query(self) -> QueryConfig:
        return self._query

    @query.setter
    def query(self, query: dict):
        try:
            self._query = QueryConfig(**query) if query else None

            if self._query:
                self._query.convert_jks_cert(self._username)

        except TypeError as e:
            self._logger.error(f"Incorrect Query configuration: {str(e)}")
            raise ValueError(f"Incorrect Query configuration: {str(e)}")

    def _validate_input(self, value: Any, attr_name: str, data_type, nullable: bool = False):
        if value is None and not nullable:
            self._logger.error(f"{attr_name} cannot be empty or None.")
            raise DBConfigError(f"{attr_name} cannot be empty or None.")
        elif value is None:
            return

        if not isinstance(value, data_type):
            self._logger.error(f"{attr_name} must be of type {data_type.__name__}.")
            raise DBConfigError(f"{attr_name} must be of type {data_type.__name__}.")

    @property
    def delicate(self) -> str:
        return self._delicate

    @delicate.setter
    def delicate(self, delicate: str):
        self._validate_input(delicate, 'delicate', str)
        self._delicate = delicate

    @property
    def host(self) -> str:
        return self._host

    @host.setter
    def host(self, host: str):
        self._validate_input(host, 'host', str)
        self._host = host

    @property
    def port(self) -> int:
        return self._port

    @port.setter
    def port(self, port: int):
        self._validate_input(port, 'port', int, nullable=True)
        if not (0 < port < 65536):
            self._logger.error("Port must be an integer between 1 and 65535.")
            raise ValueError("Port must be an integer between 1 and 65535.")
        self._port = port

    @property
    def database(self) -> Optional[str]:
        return self._database

    @database.setter
    def database(self, database: Optional[str]):
        self._validate_input(database, 'database', str, nullable=True)
        self._database = database

    @property
    def username(self) -> Optional[str]:
        return self._username

    @username.setter
    def username(self, username: Optional[str]):
        self._validate_input(username, 'username', str, nullable=True)
        self._username = username

    @property
    def password(self):
        """Returns decrypted password, encrypts it first if not already done."""
        # return self.crypto.decrypt(self._password)
        return self._password

    @password.setter
    def password(self, password: Optional[str]):
        """Encrypts and stores the password."""
        self._validate_input(password, 'password', str, nullable=True)
        # self._password = self.crypto.encrypt(password) if self.crypto and password else password
        self._password = password

    @property
    def stream(self) -> bool:
        return self._stream

    @stream.setter
    def stream(self, stream: bool):
        self._validate_input(stream, 'stream', bool)
        self._stream = stream

    @property
    def echo(self) -> bool:
        return self._echo

    @echo.setter
    def echo(self, echo: bool):
        self._validate_input(echo, 'echo', bool)
        self._echo = echo


class DBConnection:
    def __init__(self, config: DBConfig, logger=None):
        # SSH Tunnel Variables
        self.__engine = None
        self.__inspector = None
        self.__metadata = MetaData()

        self._logger = logger if logger else logging.getLogger()
        self.__config = config

        self.__create_engine()

    @classmethod
    def build_connection_from_config(cls, dbconfig: DBConfig, logger=None):
        return cls(config=dbconfig, logger=logger)

    @classmethod
    def build_connection_from_uri(cls, uri: str, logger=None, stream=False, echo=False, ):
        """Note: only works within AirFlow"""
        # Parse URI to obtain parameters for DBConfig
        # You may use a library like sqlalchemy.engine.url.make_url to parse the URI
        parsed_uri = sa.engine.url.make_url(uri)
        # Create a DBConfig instance
        dbconfig = DBConfig(
            delicate=parsed_uri.drivername,
            username=parsed_uri.username,
            password=parsed_uri.password,
            host=parsed_uri.host,
            database=parsed_uri.database,
            port=parsed_uri.port,
            query=parsed_uri.query,
            stream=stream,
            echo=echo
        )

        return cls(config=dbconfig, logger=logger)

    @classmethod
    def build_connection_from_dict(cls, config: dict, logger=None):
        # Parse URI to obtain parameters for DBConfig
        # Create a DBConfig instance
        dbconfig = DBConfig(
            delicate=config.get('delicate'),
            username=config.get('username'),
            password=config.get('password'),
            host=config.get('host'),
            database=config.get('database'),
            port=config.get('port'),
            query=config.get('query'),
            stream=config.get('stream'),
            echo=config.get('echo'),
            kerberos=config.get('kerberos'),
            logger=logger
        )

        return cls(config=dbconfig, logger=logger)

    @property
    def inspector(self):
        if self.__inspector is None:
            self._logger.info('Database engine inspector created successfully.')
            self.__inspector = sa.inspect(self.engine)
        return self.__inspector

    @property
    def metadata(self):
        return self.__metadata

    @property
    def config(self):
        return self.__config

    @config.setter
    def config(self, config: DBConfig):
        if config is not None and not isinstance(config, DBConfig):
            self._logger.error("Config must be an DBConfig instance.")
            raise TypeError("Config must be an DBConfig instance.")
        self.__config = config

    @property
    def engine(self):
        if self.__engine is None:
            self.__create_engine()
        return self.__engine

    def __create_engine(self):
        self._logger.info(f"Creating connection to {self.config.host} on {self.config.database}...")

        query = {}
        connect_args = {}
        if self.config.query is not None:
            query = self.config.query.build_db_connect_args()
            # self.config.password = None

        if self.config.kerberos is not None:
            self._logger.info(f"Kerberos {self.config.kerberos}")
            cckbs = self.config.kerberos.build_db_connect_args()
            self._logger.info(f"connect_args: {cckbs}")
            connect_args.update(cckbs)
            # query.update(connect_args)
        else:
            connect_args = {}
        self._logger.info(connect_args)
        # connect_args, query = query, connect_args

        query.update(connect_args)
        try:

            conn_url = sa.engine.url.URL(
                drivername=self.config.delicate,
                username=self.config.username,
                password=self.config.password,
                host=self.config.host,
                database=self.config.database,
                port=self.config.port,

                query=query,
            )

        except Exception as e:
            try:
                conn_url = sa.engine.url.URL.create(
                    drivername=self.config.delicate,
                    username=self.config.username,
                    password=self.config.password,
                    host=self.config.host,
                    database=self.config.database,
                    port=self.config.port,
                    query=query,
                )
            except Exception as e:
                self._logger.error(f"Failed to build a URI for the Database.")
                raise e

        self._logger.info(f'Connection URI is: {conn_url}')

        try:
            # self.__engine = create_engine(conn_url, connect_args=connect_args, echo=self.config.echo)
            self.__engine = create_engine(conn_url, echo=self.config.echo)
            if self.config.stream:
                self.engine.connect().execution_options(stream_results=self.config.stream)
            self._logger.info(f'Database [{self.engine.url.database}] session created...')
        except sa.exc.SQLAlchemyError as e:
            self._logger.error(f"Failed to create engine due database error: {e}")
            raise e
        except Exception as e:
            self._logger.error(f"Failed to create engine due unknown error: {e}")
            raise e

    def schemas(self):
        try:
            schemas = self.inspector.get_schema_names()
            df = pd.DataFrame(schemas, columns=['schema name'])
            self._logger.info(f"Number of schemas: {df.shape[0]}")
            return df
        except sa.exc.SQLAlchemyError as e:
            self._logger.error(f"Error retrieving schemas: {e}")
            raise

    def tables(self, schema: str):
        try:
            tables = self.inspector.get_table_names(schema=schema)
            df = pd.DataFrame(tables, columns=['table name'])
            self._logger.info(f"Number of tables: {df.shape[0]}")
            return df
        except sa.exc.SQLAlchemyError as e:
            self._logger.error(f"Error retrieving tables from schema {schema}: {e}")
            raise e

    def select(self, query: str, params: Optional[dict] = None, chunk_size: Optional[int] = None):
        """
        Executes a SQL select query with optional parameterization.

        :param query: SQL query string.
        :param params: Optional dictionary of parameters to be used in the query.
        :param chunk_size: Number of rows per chunk to return for large queries.
        :return: DataFrame containing the result set.
        """
        query = text(query)
        self._logger.info(f'Executing \n{query}\n in progress...')
        try:
            query_df = pd.read_sql(
                query, self.engine, params=params, chunksize=chunk_size
            ).convert_dtypes(convert_string=False)
            self._logger.info('<> Query Successful <>')
            return query_df
        except Exception as e:
            self._logger.error(f'Unable to read SQL query: {e}')
            raise e

    def insert(self, df: pd.DataFrame, table: str, schema: str,
               if_exists: Literal['fail', 'replace', 'append'] = 'fail', chunk_size: Optional[int] = 5000,
               index: bool = False, method: Literal['multi'] = 'multi', ):
        try:
            df.to_sql(
                table, self.engine, schema=schema, if_exists=if_exists, chunksize=chunk_size, index=index,
                method=method
            )
            self._logger.info(f'Data inserted into [{table}] in schema {schema} successfully.')
            return True
        except sa.exc.SQLAlchemyError as e:
            self._logger.error(f"Error inserting data into table {table}: {e}")
            raise e

    def execute(self, sql: str, commit=False):
        self._logger.info(f'Executing {sql} in progress...')
        try:
            with self.engine.connect() as conn:
                res = conn.execute(text(sql))
                if commit:
                    conn.commit()
            self._logger.info(f'<> Run SQL done Successful <>')
            return res

        except sa.exc.SQLAlchemyError as e:
            self._logger.error(f"Error executing SQL: {e}")
            return False

    def close(self):
        if self.__engine:
            self.engine.dispose()
            self._logger.info('<> Connection Closed Successfully <>')


################################################################################################################
class DBTablesFactory:
    def __init__(self, connection: DBConnection, base=None, logger=None):
        """
        Initialize the dynamic table generator with a database connection string.

        :param connection: DBConnection connection instance.
        :param logger: logger instance.
        """
        self.__connection = connection
        self.__base = base if base else declarative_base(cls=Model)
        self.__session = sessionmaker(bind=self.__connection.engine)()
        self._logger = logger if logger else logging.getLogger()

    @property
    def base(self):
        # """Provide a session context manager."""
        #
        # @contextlib.contextmanager
        # def session_scope():
        #     session = self.__session_factory()
        #     try:
        #         yield session
        #         session.commit()
        #     except Exception:
        #         session.rollback()
        #         raise
        #     finally:
        #         session.close()
        if self.__base:
            return self.__base
        else:
            self.__base = declarative_base(cls=Model)
            return self.__base

    @base.setter
    def base(self, base):
        self.__base = base

    @property
    def session(self):
        # """Provide a session context manager."""
        #
        # @contextlib.contextmanager
        # def session_scope():
        #     session = self.__session_factory()
        #     try:
        #         yield session
        #         session.commit()
        #     except Exception:
        #         session.rollback()
        #         raise
        #     finally:
        #         session.close()

        return self.__session

    def schema_exists(self, schema: str) -> bool:
        """Check if a schema exists in the database."""
        with self.__connection.engine.connect() as conn:
            return conn.dialect.has_schema(conn, schema)

    def create_schema(self, schema: str):
        """
        Create a database schema if it does not exist.

        :param schema: Name of the schema
        """
        if not self.schema_exists(schema):
            try:
                self._logger.info(f"Attempt to create schema '{schema}'.")
                with self.__connection.engine.connect() as conn:
                    conn.execute(CreateSchema(schema))
                    conn.commit()
                self._logger.info(f"Schema '{schema}' created successfully.")
                return True
            except Exception as e:
                self._logger.error(f"Error creating schema {schema}: {e}")
                raise e
        else:
            self._logger.info(f"Schema '{schema}' already exists.")
            return True

    def drop_schema(self, schema: str):
        """Drop a database schema."""
        if self.schema_exists(schema):
            try:
                self._logger.info(f"Attempt to drop schema '{schema}'.")
                with self.__connection.engine.connect() as conn:
                    conn.execute(DropSchema(schema))
                    conn.commit()
                self._logger.info(f"Schema '{schema}' dropped successfully.")
                return True
            except Exception as e:
                self._logger.error(f"Error dropping schema {schema}: {e}")
                raise
        else:
            self._logger.info(f"Schema '{schema}' does not exists.")
            return True

    def create_table_class(self, name: str, columns: dict, schema: str):
        """
        Create a SQLAlchemy table class dynamically.

        :param name: Name of the table
        :param columns: Dictionary of column names and their types
        :param schema: Schema name where the table will be created
        :return: Table class
        """
        self._logger.info(f"Initiating the table '{name}' class.")
        attrs = {
            '__tablename__': name.lower(),
            '__table_args__': {
                'extend_existing': True,
                'schema': schema
            },
        }
        attrs.update(columns)
        self._logger.info(f"'{name.capitalize()}' class created.")

        try:
            self.create_schema(schema)
            return type(name, (self.__base,), attrs)
        except Exception as e:
            self._logger.error(f"Error creating table class {name}: {e}")
            raise

    def __make_sure_schemas_exists(self, ):
        schemas = set()
        for table in self.__base.metadata.tables.values():
            if table.schema is not None:
                schemas.add(table.schema)

        for schema in schemas:
            self.create_schema(schema)

    def create_tables(self):
        """
        Create all tables in the database.
        """
        try:
            self._logger.info("Attempt to create all include schemas.")
            self.__make_sure_schemas_exists()
        except Exception as e:
            self._logger.error(f"Error creating tables schemas: {e}")
            raise e

        try:
            self._logger.info(f"Attempt to create all provided tables.")
            self.__base.metadata.create_all(self.__connection.engine)
        except Exception as e:
            self._logger.error(f"Error creating tables: {e}")
            raise e

    def get_table_metadata(self, table: str, schema: str = None) -> dict:
        """Retrieve metadata for a specified table."""
        try:
            with self.__connection.engine.connect() as conn:
                inspector = inspect(conn)
                return inspector.get_columns(table, schema=schema)
        except Exception as e:
            self._logger.error(f"Error retrieving metadata for table {table}: {e}")
            raise e

    def create_table_from_dict(self, schema: str, table: str, columns: dict):

        if not all([table, columns, schema]):
            self._logger.error("Invalid configuration. Make sure 'table_name', 'columns', and 'schema' are provided.")
            return False, None

        try:

            # Check if schema exists, if not, create it
            self.create_schema(schema)

            # Create a table class dynamically
            table_class = self.create_table_class(table, columns, schema)

            # Create tables in the database
            self.create_tables()

            path = self.dump_class_by_table_and_schema(
                cls=table_class,
                table_name=table,
                schema_name=schema,
            )

            return True, path

        except Exception as e:
            self._logger.error(f"Error creating table from config: {e}")
            raise e

    def load_class_by_table_and_schema(self, table_name, schema_name=None, path='classes'):

        source = f"{schema_name}." if schema_name else '' + table_name
        self._logger.info(f"The source tabe name is: {source}")
        if not os.path.exists(path):
            os.mkdir(path)

        file_path = os.path.join(path, f"{source}.{table_name}.ddl")

        try:
            with open(file_path, 'rb') as file:
                cls = pkl.load(file)

        except FileNotFoundError as e:
            self._logger.error(f"Unable to find the '.class' file to read the class: {e}")
            raise e
        except Exception as e:
            self._logger.error(f"Unable to : {e}")
            raise e

        return cls

    def dump_class_by_table_and_schema(self, cls, table_name, schema_name=None, path='classes'):

        source = f"{schema_name}." if schema_name else '' + table_name
        self._logger.info(f"The source tabe name is: {source}")
        if not os.path.exists(path):
            os.mkdir(path)

        file_path = os.path.join(path, f"{source}.{table_name}.ddl")

        with open(file_path, 'wb') as file:
            pkl.dump(
                cls,
                file
            )
            file.flush()

        return file_path

    def close(self):
        if self.__session:
            self.__session.close()
            self._logger.info('<> Session Closed Successfully <>')


def get_db_hook(config, base=None, logger=None, create=False):
    # if isinstance(config, str):
    #     conn = DBConnection.build_connection_from_uri(config, logger=logger)
    if isinstance(config, dict):
        conn = DBConnection.build_connection_from_dict(config, logger=logger)
    elif isinstance(config, DBConfig):
        conn = DBConnection.build_connection_from_config(config, logger=logger)
    else:
        raise TypeError(f"The provided parameter '{type(config)}' is not supported to create a database connection.")

    fac = DBTablesFactory(
        conn,
        base=base
    )

    if create:
        fac.create_tables()

    return conn, fac


if __name__ == "__main__":
    pass
