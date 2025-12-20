from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class GroupData(BaseModel):
    id: str
    name: str
    fullPath: str
    webUrl: str


class ProjectData(BaseModel):
    id: str
    name: str
    fullPath: str
    webUrl: str


class FileData(BaseModel):
    id: str
    name: str
    path: str
    webUrl: str
    sha: str


class CommitData(BaseModel):
    id: str
    created_at: str
    web_url: str
    title: str


class AuthorData(BaseModel):
    author_name: str
    author_email: Optional[str]


