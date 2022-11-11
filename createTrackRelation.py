from .createGraph import App, env

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
