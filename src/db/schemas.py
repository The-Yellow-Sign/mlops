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


class GitLabConfig(BaseModel):

    """Конфигурация для services."""

    rest_url: str = "https://gitlab.com/api/v4/projects"
    graphql_url: str = "https://gitlab.com/api/graphql"
    gitlab_token: str
    request_timeout: int = 30
    max_retries: int = 3
    retry_delay: float = 1.0
    default_branch: str = "main"


class GitLabCommitData(BaseModel):
    """Модель для валидации данных коммита от GitLab API."""

    id: str
    created_at: str
    web_url: str
    title: str
    author_name: str
    author_email: Optional[str]

