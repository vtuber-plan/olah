# coding=utf-8
# Copyright 2024 XiaHan
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

import os
from peewee import *
import datetime

from olah.utils.olah_utils import get_olah_path

db_path = os.path.join(get_olah_path(), "database.db")
db = SqliteDatabase(db_path)

class BaseModel(Model):
    class Meta:
        database = db

class Token(BaseModel):
    token = CharField(unique=True)
    first_dt = DateTimeField()
    last_dt = DateTimeField()

class DownloadLogs(BaseModel):
    id = CharField(unique=True)
    org = CharField()
    repo = CharField()
    path = CharField()
    range_start = BigIntegerField()
    range_end = BigIntegerField()
    datetime = DateTimeField()
    token = CharField()

class FileLevelLRU(BaseModel):
    org = CharField()
    repo = CharField()
    path = CharField()
    datetime = DateTimeField(default=datetime.datetime.now)

db.connect()
db.create_tables([
    Token,
    DownloadLogs,
    FileLevelLRU,
])
