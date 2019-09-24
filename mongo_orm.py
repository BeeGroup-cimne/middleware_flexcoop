from bson import InvalidDocument

from settings import MONGO_URI
from inspect import signature
from datetime import datetime
from pymongo import MongoClient
from types import MethodType

class Encoder():
    def encode(self,x):
        if type(x) == set:
            return list(x)
        else:
            return x

class AnyField():
    def __call__(self, *args, **kwargs):
        return self.var

    def __init__(self):
        self.var = 'undefined'

    def __repr__(self):
        return self.var

class MongoMeta(type):

    def __create_obj__(cls, remote):
        parameters = signature(cls.__init__).parameters
        dummy_p = {p:v.default for p,v in parameters.items() if p != 'self'}
        obj = cls(**dummy_p)
        for attr in cls.__mongo_fields__:
            obj.__setattr__(attr, remote[attr] if attr in remote else None)
        obj._id = remote['_id']
        return obj

    def __find_one__(cls, query):
        remote = cls.__mongo__.find_one(query)
        if not remote:
            return None
        return MongoMeta.__create_obj__(cls,remote)

    def __find__(cls, query):
        remote = cls.__mongo__.find(query)
        if not remote:
            return None
        return_l = []
        for r in remote:
            return_l.append(MongoMeta.__create_obj__(cls, r))
        return return_l

    def __new__(cls, name, bases, dct):
        x = super().__new__(cls, name, bases, dct)
        # set mongo connection and fields
        x.__mongo__ = MongoClient(MONGO_URI).get_database()[dct['__collectionname__']]
        x.__mongo_fields__ = [attr for attr in x.__dict__ if isinstance(x.__dict__[attr], AnyField)]
        # set variable name as string
        for attr in x.__mongo_fields__:
            x.__dict__[attr].__setattr__("var", attr)
        x.find_one = MethodType(cls.__find_one__, x)
        x.find = MethodType(cls.__find__, x)
        return x

class MongoDB(object, metaclass=MongoMeta):
    __collectionname__="dummy"

    def save(self):
        encoder = Encoder()
        obj_dict = {k: encoder.encode(v) for k,v in self.__dict__.items() if k in self.__mongo_fields__}
        try:
            try:
                obj_dict.update({"_updated_at": datetime.utcnow()})
                self.__mongo__.update_one({"_id":self._id}, {"$set": obj_dict})
            except AttributeError as e:
                obj_dict.update({"_created_at": datetime.utcnow(), "_updated_at": datetime.utcnow()})
                self._id = self.__mongo__.insert_one(obj_dict).inserted_id
        except Exception as e:
            print(type(e))
            print(e)

    def delete(self):
        self.__mongo__.delete_one({"_id":self._id})