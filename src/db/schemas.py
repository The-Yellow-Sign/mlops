from typing import Optional

from pydantic import BaseModel


class GroupData(BaseModel):

    """Модель данных для валидации информации о группе GitLab."""

    id: str
    name: str
    fullPath: str
    webUrl: str


class ProjectData(BaseModel):

    """Модель данных для валидации информации о проекте GitLab."""

    id: str
    name: str
    fullPath: str
    webUrl: str


class FileData(BaseModel):

    """Модель данных для валидации информации о файле репозитория."""

    id: str
    name: str
    path: str
    webUrl: str
    sha: str


class CommitData(BaseModel):

    """Модель данных для валидации базовой информации о коммите."""

    id: str
    created_at: str
    web_url: str
    title: str


class AuthorData(BaseModel):

    """Модель данных для валидации информации об авторе GitLab."""

    author_name: str
    author_email: Optional[str]


class GitLabConfig(BaseModel):

    """Конфигурация параметров подключения к GitLab API."""

    rest_url: str
    graphql_url: str
    gitlab_token: str
    request_timeout: int = 30
    max_retries: int = 3
    retry_delay: float = 1.0
    default_branch: str = "main"


class GitLabCommitData(BaseModel):

    """Модель для валидации полных данных коммита, полученных от GitLab API."""

    id: str
    created_at: str
    web_url: str
    title: str
    author_name: str
    author_email: Optional[str]
