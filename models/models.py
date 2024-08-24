from sqlalchemy.sql import func
from datetime import datetime

try:
    from sqlalchemy.orm import declarative_base
except:
    from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy import Column, Integer, String, DateTime, Enum, Float, Boolean, ARRAY, PrimaryKeyConstraint, \
    BigInteger, ForeignKey, Index
from models import utils
from models.consts import Status
from sqlalchemy.types import Interval
from sqlalchemy.ext.mutable import MutableList

BASE = declarative_base(cls=utils.Model)

SCHEMA = 'python'

STATUS_ENUM = Enum(Status, name='Status', schema=SCHEMA, create_type=True)


class User(BASE):  # Data Model / Model
    __tablename__ = 'users'

    email = Column(String, primary_key=True, )
    password = Column(String, nullable=False)
    # dob = Column(DateTime, nullable=True)

    # Feed outputs
    created_at = Column(DateTime, default=datetime.now, nullable=False)
    updated_at = Column(DateTime, default=datetime.now, onupdate=func.now(), nullable=False)

    __table_args__ = (
        Index(
            'idx_users',
            'email', 'password',
        ),
        {'extend_existing': True, 'schema': SCHEMA, }
    )


# class Car(BASE):  # Data Model / Model
#     __tablename__ = 'cars'
#
#     id = Column(BigInteger, primary_key=True, autoincrement=True)
#     mode = Column(String, nullable=False)
#     dob = Column(DateTime, nullable=False)
#     status = Column(STATUS_ENUM)
#     # Feed outputs
#     created_at = Column(DateTime, default=datetime.now, nullable=False)
#     updated_at = Column(DateTime, default=datetime.now, onupdate=func.now(), nullable=False)
#
#     __table_args__ = (
#         Index(
#             'idx_cars',
#             'name', 'dob',
#         ),
#         {'extend_existing': True, 'schema': SCHEMA, }
#     )
