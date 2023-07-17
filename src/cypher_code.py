cypher_conf = '''
CALL dbms.listConfig()
YIELD name, value
WHERE name STARTS WITH 'dbms.default'
RETURN name, value
ORDER BY name
LIMIT 3;
'''

cypher_info = '''
CALL dbms.listConnections() YIELD connectionId, connectTime, connector, username, userAgent, clientAddress
'''

cypher_clean = '''
MATCH (n) DETACH DELETE n
'''

cypher_node = '''MATCH (n) return count(n) '''

DIR_DATA='Users/pro/Documents/ml_with_graph_algorithms/nice_graph/neo4j_go/data'
FILENAME = 'artists_with_header.csv'

HTML_CSV = 'https://gist.githubusercontent.com/jvilledieu/c3afe5bc21da28880a30/raw/a344034b82a11433ba6f149afa47e57567d4a18f/Companies.csv'

cypher_html_csv = f'''
LOAD CSV WITH HEADERS FROM '{HTML_CSV}' AS row Return count(row);
'''

load_csv_as_row = f'''LOAD CSV WITH HEADERS FROM 'file:///{DIR_DATA}/{FILENAME}' AS row '''

cypher_csv_cnt_from_pro = f'''
{load_csv_as_row} RETURN count(row);
'''

cypher_csv_create_from_pro = f'''
LOAD CSV FROM 'file:///{DIR_DATA}/{FILENAME}' AS row 
CREATE (:Artist {{name: row[1], year: toInteger(row[2])}})
'''


cypher_csv_cnt_import = f'''
LOAD CSV WITH HEADERS FROM 'file:///{FILENAME}' AS row 
RETURN count(row);
'''

cypher_csv_limit_import = f'''
LOAD CSV WITH HEADERS FROM 'file:///{FILENAME}' AS row WITH row LIMIT 3 RETURN row;
'''

# https://neo4j.com/developer/desktop-csv-import/
# https://neo4j.com/docs/cypher-manual/current/clauses/load-csv/#load-csv-import-data-from-a-csv-file
