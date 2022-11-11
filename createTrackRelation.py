import environ
import os
from .createGraph import App
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent

env = environ.Env()
environ.Env.read_env(os.path.join(BASE_DIR, "config.env"))


if __name__ == "__main__":
    uri = env("NEO4J_URI")
    user = env("NEO4J_USERNAME")
    password = env("NEO4J_PASSWORD")

    app = App(uri, user, password)

    query_filter = {
        "skip": [0, 2],
        "limit": [2, 4]
    }

    app.createTrackRelation(query_filter)
