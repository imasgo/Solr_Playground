from peewee import *

DATABASE = 'simple.db'
database = SqliteDatabase(DATABASE)


class BaseModel(Model):
    class Meta:
        database = database


class Value(BaseModel):
    nvalue = CharField(null=True)
    fvalue = CharField()
    mdx_extra = CharField(null=True)


class Dimension(BaseModel):
    label = CharField()


class Dimension_Value(BaseModel):
    value = ForeignKeyField(Value)
    dimension = ForeignKeyField(Dimension)

    class Meta:
        primary_key = CompositeKey('value', 'dimension')


class Cube(BaseModel):
    name = CharField()
    tags = CharField()


class Cube_Value(BaseModel):
    cube = ForeignKeyField(Cube)
    value = ForeignKeyField(Value)

    class Meta:
        primary_key = CompositeKey('cube', 'value')


class Cube_Dimension(BaseModel):
    dimension = ForeignKeyField(Dimension)
    cube = ForeignKeyField(Cube)

    class Meta:
        primary_key = CompositeKey('dimension', 'cube')


class Combination(BaseModel):
    cube = ForeignKeyField(Cube)
    combination = CharField()


def create_tables():
    database.connect()
    database.create_tables(
        [Combination, Dimension, Cube, Cube_Value, Cube_Dimension, Value, Dimension_Value])


def drop_tables():
    database.drop_tables(
        [Dimension, Cube, Cube_Value, Cube_Dimension, Value, Dimension_Value, Combination])
