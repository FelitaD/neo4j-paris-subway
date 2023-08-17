from neo4j import GraphDatabase

driver = GraphDatabase.driver('bolt://0.0.0.0:7687',
                              auth=('neo4j', 'neo4j'))

delete_all = '''
MATCH (n)
DETACH DELETE n
'''

add_stations = '''
LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/pauldechorgnat/cool-datasets/master/ratp/stations.csv' AS row
CREATE (:Station {name_clean: row.nom_clean,
                  name: row.nom_gare,
                  x: toInteger(row.x),
                  y: toInteger(row.y),
                  trafic: toInteger(row.Trafic),
                  city: row.Ville,
                  ligne: row.ligne})
'''

add_liaisons_en_metro = '''
LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/pauldechorgnat/cool-datasets/master/ratp/liaisons.csv' AS row
MATCH (s1:Station) WHERE s1.name_clean = row.start
MATCH (s2:Station) WHERE s2.name_clean = row.stop
CREATE (s1)-[rel:LIAISON_METRO {ligne: row.ligne,
                                distance_km: SQRT((s2.x - s1.x)^2 + (s2.y - s1.y)^2) / 1000}]->(s2)
SET rel.time_minutes = rel.distance_km / 40 * 60
'''

add_correspondances = '''
MATCH (s1:Station)
MATCH (s2:Station)
WHERE s1.name_clean = s2.name_clean AND s1.ligne <> s2.ligne
MERGE (s1)-[c:CORRESPONDANCE {time_minutes: 4}]->(s2)
RETURN s1, type(c), s2
'''

# Note : faire un nouveau type de relation DISTANCE permet d'alléger les calculs pour certaines requêtes (notamment les
# liaisons à pieds).
# Le nouveau nombre de relations allourdit l'affichage et le temps de requêtes.
# Dans Browser Settings, décocher 'Connect result nodes' et filtrer les relations à afficher lors de la requête.
# Par ex :
# ```
# MATCH (s:Station)-[r:LIAISON_METRO|CORRESPONDANCE|LIAISON_A_PIEDS]-(:Station)
# WHERE s.ligne in ['2', '11', '3']
# RETURN s, r
# ```
add_distances = '''
MATCH (s1:Station)
MATCH (s2:Station)
CREATE (s1)-[rel:DISTANCE]->(s2)
SET rel.distance_km = SQRT((s2.x - s1.x)^2 + (s2.y - s1.y)^2) / 1000
'''

# Note: On ajoute une liaison à pied seulement quand il n'y a ni de correspondance ni de liaison en métro sur la même
# ligne.
# Il y a au moins un écueil: quand par exemple Belleville est représentée par 2 noeuds (ligne 2 et 11),
# Belleville ligne 2 a une liaison à pied avec Pyrénées (ligne 11) car ce n'est pas la même ligne.
add_liaisons_a_pieds = '''
MATCH (s1:Station)-[rel:DISTANCE]->(s2:Station)
WHERE rel.distance_km < 1 AND s1.ligne <> s2.ligne AND s1.name <> s2.name AND NOT (s1)-[:CORRESPONDANCE]->(s2)
MERGE (s1)-[l:LIAISON_A_PIEDS {distance_km: rel.distance_km, time_minutes: rel.distance_km / 4 * 60}]->(s2)
RETURN s1.name, l.distance_km, l.time_minutes, s2.name
'''

with driver.session() as session:
    session.run(delete_all)
    session.run(add_stations)
    session.run(add_liaisons_en_metro)
    session.run(add_correspondances)
    session.run(add_distances)
    session.run(add_liaisons_a_pieds)
