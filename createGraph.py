from py2neo import Graph, Node, Relationship
import environ
from pathlib import Path
from tqdm import tqdm
import os
import json
import time

BASE_DIR = Path(__file__).resolve().parent

env = environ.Env()
environ.Env.read_env(os.path.join(BASE_DIR, "config.env"))


def load_json(path_n_name):
    with open(path_n_name, 'r') as f:
        data = json.loads(f.read())
        # print("=>>", path_n_name, "has been loaded ")
    return data


class App:

    def __init__(self, uri, user, password):
        self.driver = Graph(uri, user=user, password=password)

    def create_node(self, label, properties):
        node = Node(label, **properties)
        self.driver.create(node)
        # print(f"creating node: {node}")
        return node

    def add_relation(self, src_node, relationship, to_node):
        relation = Relationship(src_node, relationship, to_node)
        self.driver.create(relation)
        # print(f"creating relation {relationship} between {src_node}-{to_node}")

    def delete_all_node_and_relation(self, label):
        query = f"MATCH (n:{label}) DETACH DELETE n;"
        self.driver.run(query)

    def createTrackRelation(self, filter_query: dict = None):
        all_filter = ""

        if filter_query != None:
            all_filter = []

            filter_query_1 = f"""
                                SKIP {filter_query["skip"][0]} 
                                LIMIT {filter_query["limit"][0]}
                                """
            filter_query_2 = f"""
                                SKIP {filter_query["skip"][1]} 
                                LIMIT {filter_query["limit"][1]}
                                """
            all_filter.append(filter_query_1)
            all_filter.append(filter_query_2)

        query = f"""
                
                MATCH (f1:Frame) WITH f1 
                ORDER BY f1.timestamp 
                {all_filter[0]}

                MATCH (f2:Frame) WITH f1, f2 
                ORDER BY f2.timestamp 
                {all_filter[1]}

                MATCH (f1:Frame)<--(o1:Object) WITH f1, f2, o1
                MATCH (f2:Frame)<--(o2:Object) WITH f1, f2, o1, o2        
                
                WHERE NOT (o1)-[:Track]-(o2)
                AND o1.id_track=o2.id_track
                AND o1<>o2
                AND f1.timestamp < f2.timestamp

                MERGE o1-[:Track]->(o2)

                """

        self.driver.run(query)


if __name__ == "__main__":
    # Aura queries use an encrypted connection using the "neo4j+s" URI scheme
    uri = env("NEO4J_URI")
    user = env("NEO4J_USERNAME")
    password = env("NEO4J_PASSWORD")

    app = App(uri, user, password)

    FRAME_LABEL = "Frame"
    OBJECT_LABEL = "Object"
    FRAME_OBJECT_RELATIONSHIP_LABEL = "INSIDE"

    app.delete_all_node_and_relation(FRAME_LABEL)
    app.delete_all_node_and_relation(OBJECT_LABEL)

    data = load_json(os.path.join(BASE_DIR, "dummy_data.json"))

    latency_creating_frame = []
    latency_creating_object = []
    latency_creating_relation = []

    init_time_process = time.time()
    for i, data in enumerate(tqdm(data)):
        job = data['job']

        frame_properties = {
            'name': 'frame' + '_' + str(job['frame_id']),
            'status': job['status'],
            'frame_id': job['frame_id'],
            'timestamp': job['timestamp'],
            'cam_id': job['cam_id'],
            'lat': job['lat'],
            'long': job['long'],
            'traffic_density': job['traffic_density']
        }

        init_time = time.time()
        # add frame node
        frame_node = app.create_node(FRAME_LABEL, properties=frame_properties)

        latency_creating_frame.append(time.time() - init_time)
        # print(f"latency creting Frame node: {time.time() - init_time}")

        objects = job['result']

        for obj in objects:
            # create object node properties
            object_properties = obj
            object_properties['name'] = obj['class'] + \
                '_' + str(obj['id_track'])

            init_time_object = time.time()
            # create object node
            object_node = app.create_node(
                OBJECT_LABEL, properties=object_properties)

            latency_creating_object.append(time.time() - init_time_object)
            # print(
            #     f"latency time object node: {time.time() - init_time_object}")

            init_time_relation = time.time()
            # create frame-object relationship
            app.add_relation(
                object_node, FRAME_OBJECT_RELATIONSHIP_LABEL, frame_node)

            latency_creating_relation.append(time.time() - init_time_relation)
            # print(
            #     f"latency creating relation: {time.time() - init_time_relation}")
    print(
        f"latency creating all process : {time.time() - init_time_process}")
    print(
        f"average latency creating Frame: {round(sum(latency_creating_frame)/len(latency_creating_frame), 4)}")
    print(
        f"average latency creating Object: {round(sum(latency_creating_object)/len(latency_creating_object), 4)}")
    print(
        f"average latency creating Relation: {round(sum(latency_creating_relation)/len(latency_creating_relation), 4)}")
