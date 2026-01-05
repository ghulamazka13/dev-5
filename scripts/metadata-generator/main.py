"""Simple CLI to run the metadata generator that reads Redis and emits DAG JSON files."""
import argparse
import logging
from airflow.dags.generator import generate_from_redis


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--redis-host", default="metadata-redis")
    parser.add_argument("--redis-port", default=6379, type=int)
    parser.add_argument("--redis-db", default=0, type=int)
    parser.add_argument("--redis-key", default="pipelines")
    parser.add_argument("--out-dir", default=None)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    generate_from_redis(redis_host=args.redis_host, redis_port=args.redis_port, redis_db=args.redis_db, redis_key=args.redis_key, out_dir=args.out_dir)


if __name__ == "__main__":
    main()
