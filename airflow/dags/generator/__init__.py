"""Generator package: creates DAG artifacts from metadata stored in Redis."""

from .metadata_generator import generate_from_redis

__all__ = ["generate_from_redis"]
