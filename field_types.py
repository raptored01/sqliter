import datetime

from exceptions import ForeignKeyError


class FieldTypes:

    @staticmethod
    def text(value):
        return str(value)

    @staticmethod
    def integer(value):
        return int(value)

    @staticmethod
    def real(value):
        return float(value)

    @staticmethod
    def blob(value):
        return bytes(value)

    @staticmethod
    def date(value):
        return datetime.datetime.strptime(value, "%Y-%m-%d").date()

    @staticmethod
    def datetime(value):
        try:
            return datetime.datetime.strptime(value, "%Y-%m-%d %H:%M:%S.%f")
        except ValueError:
            return datetime.datetime.strptime(value, "%Y-%m-%d %H:%M:%S")

    @staticmethod
    def boolean(value):
        return bool(value)


FIELD_TYPES = {
    "TEXT": FieldTypes.text,
    "INTEGER": FieldTypes.integer,
    "REAL": FieldTypes.real,
    "DATE": FieldTypes.date,
    "DATETIME": FieldTypes.datetime,
    "BLOB": FieldTypes.blob,
    "BOOLEAN": FieldTypes.boolean,
}

TYPE_MAP = {
    "TEXT": str,
    "INTEGER": int,
    "REAL": float,
    "DATE": type(datetime.datetime.now().date()),
    "DATETIME": type(datetime.datetime.now()),
    "BLOB": bytes,
    "BOOLEAN": bool
}

TYPE_MAP_REV = {
    str: "TEXT",
    int: "INTEGER",
    float: "REAL",
    type(datetime.datetime.now().date()): "DATE",
    type(datetime.datetime.now()): "DATETIME",
    bytes: "BLOB",
    bool: "BOOLEAN"
}


class Field:
    def __init__(self, field_type, null=True, default=None, pk=False, autoincrement=True, unique=False,
                 fk=False, **kwargs):
        assert isinstance(null, bool), "null param must be of type bool"
        self.null = null
        self.default = default
        assert isinstance(pk, bool), "pk param must be of type bool"
        self.pk = pk
        assert isinstance(autoincrement, bool), "autoincrement param must be of type bool"
        self.autoincrement = autoincrement
        assert isinstance(unique, bool), "unique param must be of type bool"
        self.unique = unique

        if default is not None:
            assert isinstance(default, field_type), f"default value type ({type(default)}) " \
                                                    f"and field type ({field_type}) not matching)"
        assert isinstance(fk, bool), "fk param must be of type bool"
        self.fk = fk
        if self.fk:
            assert "references" in kwargs, "must provide reference for Foreign Key"
            try:
                self.references = kwargs["references"].name, kwargs["references"].pk
            except AttributeError:
                try:
                    self.references = kwargs["references"]["table"], kwargs["references"]["column"]
                except KeyError:
                    try:
                        self.references = kwargs["references"][0], kwargs["references"][1]
                    except (IndexError, TypeError):
                        raise ForeignKeyError("ForeignKey reference must be either:\n"
                                              "\tTable instance\n"
                                              "\t{table: table_name, column: column_name} dict\n"
                                              "\t(table_name, column_name) tuple\n"
                                              f"Received: {type(kwargs['references'])}")
            assert "on_delete" in kwargs, "must provide action for on_delete event"
            self.on_delete = kwargs["on_delete"]

    def sql(self, name):
        sql = f"""{name} {self.typename}"""
        if self.pk:
            sql += " PRIMARY KEY"
            if self.typename == "INTEGER" and self.autoincrement:
                sql += " AUTOINCREMENT"

        if self.unique:
            sql += " NOT NULL UNIQUE"

        if self.null is False and self.unique is False and self.pk is False:
            sql += " NOT NULL"

        if self.fk:
            ref_table, ref_column = self.references
            sql += f" REFERENCES {ref_table}({ref_column}) ON DELETE {self.on_delete}"

        if self.default is not None:
            sql += f" DEFAULT '{self.default}'"  # huge security risk!

        return sql

    def __repr__(self):
        return self.sql("")


class Fields:

    # {'cid': 0, 'name': 'id', 'type': 'INTEGER', 'notnull': 0, 'dflt_value': None, 'pk': 1}
    CASCADE = "CASCADE"
    NO_ACTION = "NO ACTION"
    RESTRICT = "RESTRICT"
    SET_NULL = "SET NULL"
    SET_DEFAULT = "SET DEFAULT"

    class Text(Field):
        def __init__(self, null=True, default=None, pk=False, autoincrement=True, unique=False):
            super().__init__(str, null, default, pk, autoincrement, unique)
            self.typename = "TEXT"
            self.type = str

    class Integer(Field):
        def __init__(self, null=True, default=None, pk=False, autoincrement=True, unique=False):
            super().__init__(int, null, default, pk, autoincrement, unique)
            self.typename = "INTEGER"
            self.type = int

    class Real(Field):
        def __init__(self, null=True, default=None, pk=False, autoincrement=True, unique=False):
            super().__init__(float, null, default, pk, autoincrement, unique)
            self.typename = "REAL"
            self.type = float

    class Blob(Field):
        def __init__(self, null=True, default=None, pk=False, autoincrement=True, unique=False):
            super().__init__(bytes, null, default, pk, autoincrement, unique)
            self.typename = "BLOB"
            self.type = bytes

    class Date(Field):
        def __init__(self, null=True, default=None, pk=False, autoincrement=True, unique=False):
            super().__init__(type(datetime.datetime.now().date()), null, default, pk, autoincrement, unique)
            self.typename = "DATE"
            self.type = type(datetime.datetime.now().date())

    class DateTime(Field):
        def __init__(self, null=True, default=None, pk=False, autoincrement=True, unique=False):
            super().__init__(type(datetime.datetime.now()), null, default, pk, autoincrement, unique)
            self.typename = "DATETIME"
            self.type = type(datetime.datetime.now())

    class ForeignKey(Field):
        def __init__(self, references, on_delete, field_type=int,
                     null=True, default=None, pk=False, autoincrement=True, unique=False):

            super().__init__(field_type, null, default, pk, autoincrement, unique,
                             fk=True, on_delete=on_delete, references=references)

            self.typename = TYPE_MAP_REV[field_type]
            self.type = field_type
