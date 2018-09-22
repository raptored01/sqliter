import datetime


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
