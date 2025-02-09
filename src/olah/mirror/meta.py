# coding=utf-8
# Copyright 2024 XiaHan
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.


from typing import Any, Dict


class RepoMeta(object):
    def __init__(self) -> None:
        self._id = None
        self.id = None
        self.author = None
        self.sha = None
        self.lastModified = None
        self.private = False
        self.gated = False
        self.disabled = False
        self.tags = []
        self.description = ""
        self.paperswithcode_id = None
        self.downloads = 0
        self.likes = 0
        self.cardData = None
        self.siblings = None
        self.createdAt = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "_id": self._id,
            "id": self.id,
            "author": self.author,
            "sha": self.sha,
            "lastModified": self.lastModified,
            "private": self.private,
            "gated": self.gated,
            "disabled": self.disabled,
            "tags": self.tags,
            "description": self.description,
            "paperswithcode_id": self.paperswithcode_id,
            "downloads": self.downloads,
            "likes": self.likes,
            "cardData": self.cardData,
            "siblings": self.siblings,
            "createdAt": self.createdAt,
        }
