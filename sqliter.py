import sqlite3

from exceptions import (
    InvalidFieldName,
    MismatchingTypes,
    NoSuchField,
    NoSuchTable,
    NoSuchEntry,
    UnknownOperation,
)

from field_types import TYPE_MAP, FIELD_TYPE_ENFORCERS, Fields
from utils import is_valid_field_name, clean_kwargs, scrub, types_match


__all__ = ["Database", "Fields"]


class Database:

    def __init__(self, __db_name):
        self.db_name = __db_name
        self.connection = sqlite3.connect(__db_name)
        self.connection.row_factory = sqlite3.Row
        self.cursor = self.connection.cursor()
        with self.connection:
            self.cursor.execute("PRAGMA FOREIGN_KEYS = ON")
            # for some weird reason, this is needed
            # otherwise the foreign keys are not enforced

    def raw(self, sql):
        self.cursor.execute(sql)
        return self.cursor.lastrowid

    @property
    def tables(self):
        sql = """SELECT name FROM sqlite_master WHERE type='table'"""
        return [row["name"] for row in self.cursor.execute(sql)]

    def table(self, name):
        if name in self.tables:
            return Table(self, name)
        else:
            raise NoSuchTable(name)

    def __create_table(self, table_name, condition, **kwargs):
        table_name = scrub(table_name)
        fields = ", ".join(field.sql(key) for key, field in kwargs.items())
        create_statement = f"""
        CREATE TABLE {condition} {table_name}(
        {fields}
        )
        """
        with self.connection:
            self.cursor.execute(create_statement)
        return Table(self, table_name)

    def create_table(self, table_name, **kwargs):
        return self.__create_table(table_name, "", **kwargs)

    def create_table_if_not_exists(self, table_name, **kwargs):
        return self.__create_table(table_name, "IF NOT EXISTS", **kwargs)


class Table:

    # TODO: alter

    def __init__(self, __database, __name):
        self.database = __database
        self.connection = __database.connection
        self.__cursor = self.connection.cursor()
        self.name = __name
        self.fields, self.pk = self.__get_fields()
        self.columns = list(self.fields.keys())
        self.foreign_keys = self.__get_foreign_keys()

    def all(self):
        return QuerySet(self)

    def __create(self, condition, bulk=False, **kwargs):
        # todo: auto-add pk if not provided
        kwargs = clean_kwargs(**kwargs)
        for k, v in kwargs.items():
            if isinstance(v, Entry):
                kwargs[k] = v._pk
        cols = ", ".join(kwargs.keys())
        values = ", ".join(f":{kw}" for kw in kwargs.keys())
        sql = f"""
        INSERT {condition} INTO {self.name} (
            {cols}
        ) VALUES ({values})
        """
        if not bulk:
            with self.connection:
                self.__cursor.execute(sql, kwargs)
                entry = self.get(rowid=self.__cursor.lastrowid)
                return entry
        else:
            self.__cursor.execute(sql, kwargs)

    def create(self, **kwargs):
        return self.__create(condition="", **kwargs)

    def create_or_replace(self, **kwargs):
        return self.__create(condition="OR REPLACE", **kwargs)

    def create_or_ignore(self, **kwargs):
        return self.__create(condition="OR IGNORE", **kwargs)

    def __bulk_create(self, condition, iterable):
        assert hasattr(iterable, '__iter__'), f"You must provide an iterable of dicts, not {type(iterable)}"
        with self.connection:
            for kwargs in iterable:
                assert isinstance(kwargs, dict), f"You must provide an iterable of dicts, not of {type(kwargs)}"
                self.__create(condition, bulk=True, **kwargs)

    def bulk_create(self, iterable):
        self.__bulk_create("", iterable)

    def bulk_create_or_replace(self, iterable):
        self.__bulk_create("OR REPLACE", iterable)

    def bulk_create_or_ignore(self, iterable):
        self.__bulk_create("OR IGNORE", iterable)

    def clear(self):
        with self.connection:
            self.__cursor.execute(f"DELETE FROM {self.name}")

    def drop(self):
        with self.connection:
            self.__cursor.execute(f"DROP TABLE {self.name}")

    def filter(self, operator="AND", **kwargs):
        return QuerySet(self, operator=operator, **kwargs)

    def get(self, **kwargs):
        assert len(kwargs) > 0, "You must provide **kwargs"
        kwargs = clean_kwargs(**kwargs)
        if "pk" in kwargs:
            kwargs[self.pk] = kwargs.pop("pk")
        conditions = " AND ".join(f"{key}=:{key}" for key in kwargs)
        sql = f"""
            SELECT * FROM {self.name}
            WHERE {conditions}
            """
        row = self.__cursor.execute(sql, kwargs).fetchone()
        if row is None:
            raise NoSuchEntry
        entry = Entry(self, row[self.pk], **row)
        return entry

    @property
    def __table_info(self):
        return [dict(row)
                for row in self.__cursor.execute(f"PRAGMA table_info({self.name})")]

    def __get_fields(self):
        fields = {pragma["name"]: pragma for pragma in self.__table_info}
        pk = None
        for name, pragma in fields.items():
            pragma["enforce_type"] = FIELD_TYPE_ENFORCERS[pragma["type"]]
            pragma["type"] = TYPE_MAP[pragma["type"]]
            if pragma["pk"] == 1:
                pk = name
        return fields, pk

    def __get_foreign_keys(self):
        return {row['from']: dict(row)
                for row in self.__cursor.execute(f"PRAGMA foreign_key_list({self.name})")}


class Entry:

    def __init__(self, __table, __pk, **kwargs):
        self.__table = __table
        self.__database = self.__table.database
        self.__connection = self.__table.connection
        self.__cursor = self.__connection.cursor()
        self._pk = __pk
        self.__instanciate(**kwargs)

    def __instanciate(self, **kwargs):
        foreign_keys = self.__table.foreign_keys
        for key, value in kwargs.items():
            if not is_valid_field_name(key):
                raise InvalidFieldName(key)
            if key in foreign_keys:
                table, to = foreign_keys[key]["table"], foreign_keys[key]["to"]
                if isinstance(value, Entry):
                    value = value._pk
                value = self.__database.table(table).get(**{to: value})
                setattr(self, key, value)
            else:
                setattr(self, key, self.__table.fields[key]["enforce_type"](value))

    def __reload(self):
        row = self.__cursor.execute(
            f"""
            SELECT * FROM {self.__table.name}
            WHERE {self.__table.pk}=?
            """, (self._pk,)
        ).fetchone()
        if row is None:
            raise NoSuchEntry(self.__table.name, self._pk)
        self.__instanciate(**row)

    def save(self):
        """
        Updates itself writing changes to the db
        """
        kwargs = {}
        for key in self.__table.fields:
            if isinstance(self.__dict__[key], Entry):   # if a value it's another Entry object
                kwargs[key] = self.__dict__[key]._pk    # we translate it to its primary key
            else:
                kwargs[key] = self.__dict__[key]        # else we write it as it is
            if not types_match(kwargs[key], key, self.__table.fields):
                raise MismatchingTypes(f"Received {type(kwargs[key])}, Expected {self.__table.fields[key]['type']}")

        kwargs[self.__table.pk] = self._pk
        values = ", ".join([f"{key}=:{key}" for key in kwargs if key != "{self.__table.pk}"])
        sql = f"""
            UPDATE {self.__table.name}
            SET {values}
            WHERE {self.__table.pk}=:{self.__table.pk}
            """
        with self.__connection:
            self.__cursor.execute(sql, kwargs)

    def delete(self):
        with self.__connection:
            self.__cursor.execute(
                f"""
                DELETE FROM {self.__table.name}
                WHERE {self.__table.pk}=?
                """, (self._pk,)
            )

    def __repr__(self):
        return f"<Entry {str({key: self.__dict__[key] for key in self.__table.columns})} >"


class QuerySet:

    __QUERY_DICT = {
        "equals": lambda x: f"= :__{x}",
        "gt": lambda x: f"> :__{x}",
        "lt": lambda x: f"< :__{x}",
        "gte": lambda x: f">= :__{x}",
        "lte": lambda x: f"<= :__{x}",
        "like": lambda x: f"LIKE :__{x}",
        "ilike": lambda x: f"LIKE :__{x}",
        "contains": lambda x: f"LIKE :__{x}",
        "icontains": lambda x: f"LIKE :__{x}",
        "in": lambda x: f"IN [:__{x}]",
    }

    __VALUES_DICT = {
        "ilike": lambda x: f"%{x}%",
        "icontains": lambda x: f"%{x}%"
    }

    def __init__(self, __table, operator="AND", **kwargs):
        self.__table = __table
        self.__connection = self.__table.connection
        self.__cursor = self.__connection.cursor()
        assert operator.lower() in ("and", "or"), f"Unknown operator: {operator}"
        self.__operator = operator.upper()
        self.__query = {}
        self.__kwargs = {}
        self.__orderby = []

        kwargs = clean_kwargs(**kwargs)
        for key, value in kwargs.items():
            field_query = key.split("__")
            if len(field_query) == 2:
                field, query = field_query
                if query not in self.__QUERY_DICT:
                    raise UnknownOperation(query)
            else:
                field, query = key, "equals"
            if field == "pk":
                field = self.__table.pk
            if field not in self.__table.fields:
                raise NoSuchField(field)
            if not types_match(value, field, self.__table.fields):
                raise MismatchingTypes(f"{field}: expected {self.__table.fields[field]['type']}, got {type(value)}")
            self.__query[field] = self.__QUERY_DICT[query]
            if isinstance(value, list) or isinstance(value, tuple):
                value = ", ".join(str(x) for x in value)
            self.__kwargs[f"__{field}"] = self.__VALUES_DICT.get(query, lambda x: x)(value)

    def delete(self):
        with self.__connection:
            self.__cursor.execute(f"DELETE FROM {self.__table.name} {self.__condition_statement}", self.__kwargs)

    def order_by(self, field: str):
        order = "ASC"
        if field.startswith("-"):
            order = "DESC"
        field = field.replace("-", "")
        if field not in self.__table.fields:
            raise NoSuchField(field)
        self.__orderby.append((field, order))
        return self

    def update(self, **kwargs):
        for key, value in kwargs.items():
            if key not in self.__table.fields:
                raise NoSuchField(key)
            if isinstance(value, Entry):  # if a value it's another Entry object
                kwargs[key] = value._pk  # we translate it to its primary key
            if not types_match(value, key, self.__table.fields):
                raise MismatchingTypes(f"Received {type(value)}, Expected {self.__table.fields[key]['type']}")

        values = ", ".join([f"{key}=:{key}" for key in kwargs if key != "self.__table.pk"])
        sql = f"""
            UPDATE {self.__table.name}
            SET {values}
            {self.__condition_statement}
            """
        kwargs.update(**self.__kwargs)
        with self.__connection:
            self.__cursor.execute(sql, kwargs)

    def first(self):
        row = self.__cursor.execute(self.__select_statement, self.__kwargs).fetchone()
        if row is None:
            return None
        return Entry(self.__table, row[self.__table.pk], **row)

    @property
    def __condition_statement(self):
        conditions = []
        if self.__query:
            for key, query in self.__query.items():
                conditions.append(f"{key} {query(key)}")
            conditions_statement = f" {self.__operator} ".join(conditions)
            return f"WHERE {conditions_statement}"
        else:
            return ""

    @property
    def __orderby_statement(self):
        orderby_statements = ", ".join(f"{field} {order}" for field, order in self.__orderby)
        if orderby_statements:
            return f"ORDER BY {orderby_statements}"
        else:
            return ""

    @property
    def __select_statement(self):
        return f"""SELECT * FROM {self.__table.name}
               {self.__condition_statement}
               {self.__orderby_statement}
               """

    def __iter__(self):
        for row in self.__cursor.execute(self.__select_statement, self.__kwargs):
            yield Entry(self.__table, row[self.__table.pk], **row)
