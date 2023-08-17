import argparse
from neo4j import GraphDatabase


class Dijkstra:
    def __init__(self, start, end):
        self.driver = GraphDatabase.driver('bolt://0.0.0.0:7687', auth=('neo4j', 'neo4j'))
        self.start = start
        self.end = end

    def name_graph(self):
        query = '''
            CALL gds.graph.create(
                'Paris',
                'Station',
                ['LIAISON_METRO', 'LIAISON_A_PIEDS', 'CORRESPONDANCE'],
                {
                    relationshipProperties: 'time_minutes'
                }
            )
        '''
        with self.driver.session() as session:
            session.run(query)

    def find_shortest_path(self):
        shortest_path = '''
            MATCH (s1:Station {start_node}),
                  (s2:Station {end_node})
            CALL gds.alpha.shortestPath.stream('Paris',{{
              startNode:s1,
              endNode:s2,
              relationshipWeightProperty:'time_minutes'}})
            YIELD nodeId, cost
            RETURN distinct(gds.util.asNode(nodeId).name) as station, cost
        '''.format(start_node=f'{{name_clean:"{self.start}"}}', end_node=f'{{name_clean:"{self.end}"}}')

        with self.driver.session() as session:
            r = session.run(shortest_path)
            return [dict(i) for i in r]


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Find shortest path between two Paris stations.')
    parser.add_argument('--start', type=str, default='STLAZARE',
                        help='name_clean of starting station')
    parser.add_argument('--end', type=str, default='PYRENEES',
                        help='name_clean of ending station')
    args = parser.parse_args()

    dijkstra = Dijkstra(args.start, args.end)
    dijkstra.name_graph()
    shortest_path = dijkstra.find_shortest_path()
    print(shortest_path)
