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


class User(BaseModel):
    username = CharField(unique=True)
