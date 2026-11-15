"""Shared Aura connection settings, read from .env (never logged)."""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv
from neo4j import AsyncGraphDatabase, Driver, GraphDatabase
from neo4j import AsyncDriver

load_dotenv(Path(__file__).resolve().parents[1] / ".env")

DATABASE = os.environ.get("NEO4J_DATABASE", "neo4j")


def _auth() -> tuple[str, str]:
    return os.environ["NEO4J_USERNAME"], os.environ["NEO4J_PASSWORD"]


def driver(**kw: object) -> Driver:
    return GraphDatabase.driver(os.environ["NEO4J_URI"], auth=_auth(), **kw)


def async_driver(**kw: object) -> AsyncDriver:
    return AsyncGraphDatabase.driver(os.environ["NEO4J_URI"], auth=_auth(), **kw)
