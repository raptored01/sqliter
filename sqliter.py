import sqlite3

from exceptions import (
    InvalidFieldName,
    NoSuchTable,
    NoSuchEntry,
)

from field_types import FIELD_TYPES, Fields
from utils import is_valid_field_name, clean_kwargs, scrub


__all__ = ["Database"]


import os
os.system("rm test.db")


class Database:

    def __init__(self, __db_name):
        self.db_name = __db_name
        self.connection = sqlite3.connect(__db_name)
        self.connection.row_factory = sqlite3.Row
        self.cursor = self.connection.cursor()

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

    def create_table(self, table_name, **kwargs):
        table_name = scrub(table_name)
        fields = ", ".join(field.sql(key) for key, field in kwargs.items())
        create_statement = f"""
        CREATE TABLE {table_name}(
        {fields}
        )
        """
        with self.connection:
            self.cursor.execute(create_statement)
        fk = {}
        for key, field in kwargs.items():
            if hasattr(field, "references"):
                fk[key] = field.references
        return Table(self, table_name, **fk)


class Table:

    def __init__(self, __database, __name, **fk):
        self.database = __database
        self.connection = __database.connection
        self.__cursor = self.connection.cursor()
        self.name = __name
        self.fields, self.pk = self.__get_fields()
        self.fk = fk

    def all(self, *columns):
        cols = ", ".join(columns) or "*"
        for row in self.__cursor.execute(f"""SELECT {cols} FROM {self.name}"""):
            yield Entry(self, row[self.pk], **row)

    def create(self, **kwargs):
        kwargs = clean_kwargs(**kwargs)
        for k, v in kwargs.items():
            if isinstance(v, Entry):
                kwargs[k] = v._pk
        cols = ", ".join(kwargs.keys())
        values = ", ".join(f":{kw}" for kw in kwargs.keys())
        sql = f"""
                INSERT INTO {self.name} (
                    {cols}
                ) VALUES ({values})
                """
        with self.connection:
            self.__cursor.execute(sql, kwargs)
            entry = self.get(rowid=self.__cursor.lastrowid)
            return entry

    def create_or_replace(self, **kwargs):
        kwargs = clean_kwargs(**kwargs)
        for k, v in kwargs.items():
            if isinstance(v, Entry):
                kwargs[k] = v._pk
        cols = ", ".join(kwargs.keys())
        values = ", ".join(f":{kw}" for kw in kwargs.keys())
        sql = f"""
                INSERT OR REPLACE INTO {self.name} (
                    {cols}
                ) VALUES ({values})
                """
        with self.connection:
            self.__cursor.execute(sql, kwargs)
            entry = self.get(rowid=self.__cursor.lastrowid)
            return entry

    def create_or_ignore(self, **kwargs):
        kwargs = clean_kwargs(**kwargs)
        for k, v in kwargs.items():
            if isinstance(v, Entry):
                kwargs[k] = v._pk
        cols = ", ".join(kwargs.keys())
        values = ", ".join(f":{kw}" for kw in kwargs.keys())
        sql = f"""
                INSERT OR IGNORE INTO {self.name} (
                    {cols}
                ) VALUES ({values})
                """
        with self.connection:
            self.__cursor.execute(sql, kwargs)
            entry = self.get(rowid=self.__cursor.lastrowid)
            return entry

    def clear(self):
        with self.connection:
            self.__cursor.execute(
                f"""
                DELETE FROM {self.name}
                """
            )

    @property
    def columns(self):
        cursor = self.connection.execute(f"SELECT * FROM {self.name}")
        return list(map(lambda x: x[0], cursor.description))

    def filter(self, **kwargs):
        # TODO
        pass

    def get(self, **kwargs):
        assert len(kwargs) > 0, "You must provide **kwargs"
        kwargs = clean_kwargs(**kwargs)
        print(self.name, kwargs)
        if "pk" in kwargs:
            kwargs["id"] = kwargs["pk"]
            kwargs.pop("pk")
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
    def pragma(self):
        return [dict(row)
                for row in self.__cursor.execute(f"PRAGMA table_info({self.name})")]

    def __get_fields(self):
        fields = {pragma["name"]: pragma for pragma in self.pragma}
        pk = None
        for name, pragma in fields.items():
            pragma["type"] = FIELD_TYPES[pragma["type"]]
            if pragma["pk"] == 1:
                pk = name
        return fields, pk


class Entry:

    def __init__(self, __table, __pk, **kwargs):
        self.__table = __table
        self.__database = self.__table.database
        self.__connection = self.__table.connection
        self.__cursor = self.__connection.cursor()
        self._pk = __pk
        self.__instanciate(**kwargs)

    def __instanciate(self, **kwargs):
        fk = self.__table.fk
        for key, value in kwargs.items():
            if not is_valid_field_name(key):
                raise InvalidFieldName(key)
            if key in fk:
                tname, cname = fk[key]
                value = self.__database.table(tname).get(**{cname: value})
            setattr(self, key, value)

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

    def __is_protected_name(self, name):
        protected_name = f"_{self.__class__.__name__}__"
        return name.startswith(protected_name)

    def save(self):
        kwargs = {}
        for key, value in self.__table.fields.items():
            if isinstance(self.__dict__[key], Entry):
                kwargs[key] = self.__dict__[key]._pk
            else:
                kwargs[key] = self.__dict__[key]
        kwargs[self.__table.pk] = self._pk
        # print(kwargs)
        values = ", ".join([f"{key}=:{key}" for key in kwargs if key != "id"])
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
        return str({key: self.__dict__[key] for key in self.__table.columns})


# connect
my_db = Database("test.db")

# create a new table without getting its instance
my_db.create_table(
    "owners",
    id=Fields.Integer(pk=True),
    name=Fields.Text(null=False),
)

# get an existing table
owners = my_db.table("owners")

# create new entries
frank = owners.create(name="Frank")
josh = owners.create(name="Josh")
jenny = owners.create(name="Jenny")
anna = owners.create(name="Anna")

# create a new table getting its instance
dogs = my_db.create_table(
    "dogs",
    id=Fields.Integer(pk=True),
    name=Fields.Text(null=False),
    age=Fields.Integer(null=False),
    owner=Fields.ForeignKey(references=owners, on_delete=Fields.CASCADE)
)

# create new entries
dogs.create(name="Fido", age=3, owner=frank)
dogs.create(name="Jo", age=1, owner=2)
dogs.create(name="Luna", age=5, owner=jenny)
dogs.create(name="Dina", age=1, owner=4)


for dog in dogs.all():
    print(dog)
    dog.owner = frank
    dog.save()
    print(dog)
    print()

