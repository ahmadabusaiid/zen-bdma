from pathlib import Path
import os
import sys
from neo4j import GraphDatabase

sys.path.insert(0, os.path.dirname(Path(__file__).parent.absolute()))
import configs.common as common

class GDBConnector:

    def __init__(self):
        
        self._host = common.neo4j['host_path']
        self._username = common.neo4j['user']
        self._password = common.neo4j['password']
        self._conn = None

    def get_conn(self):
        
        if self._conn == None :
            self._conn = GraphDatabase.driver(self._host, auth=(self._username, self._password))
        return self._conn
    
    def close_conn(self):
        self._conn.close()
        self._conn = None

connector = GDBConnector()

def get_communities():
    
    conn = connector.get_conn()
    with conn.session() as session:
        result = session.run("""
            CALL gds.louvain.stream('ExpiringProducts', {
            relationshipWeightProperty: 'count',
            includeIntermediateCommunities: True
            }) YIELD nodeId, communityId, intermediateCommunityIds
            WITH gds.util.asNode(nodeId).id AS Name, gds.util.asNode(nodeId).label AS Label, communityId, intermediateCommunityIds
            RETURN Name, Label, intermediateCommunityIds
        """)
        names = [record["Name"] for record in result]
    connector.close_conn()
    print(names)
    return names