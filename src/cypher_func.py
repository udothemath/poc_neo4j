from .cypher_code import cypher_clean, cypher_conf

def cypher_property_code(the_file:str):
    cypher_constraint_node_id = '''
        CREATE CONSTRAINT IF NOT EXISTS ON (m:ID) ASSERT (m.node_id) IS UNIQUE
    '''
    cypher_index_node_id = """
        CREATE INDEX node_index_name IF NOT EXISTS FOR (n:ID) ON (n.node_id)
    """

    cypher_property = f'''
        USING PERIODIC COMMIT 5000
        LOAD CSV WITH HEADERS FROM 'file:///{the_file}' AS row
        MERGE (id:ID {{name: row.node_id}} )
        ON MATCH SET 
        id.gender=row.gender,
        id.address=row.address
        RETURN count(id);
    '''
    cypher_list = [
        cypher_constraint_node_id,
        cypher_index_node_id,
        cypher_property
    ]
    return cypher_list


def cypher_node_code(the_file:str):

    cypher_constraint_id_from_to = '''
        CREATE CONSTRAINT IF NOT EXISTS ON (m:ID) ASSERT (m.from, m.to) IS UNIQUE
    '''
    cypher_index1 = """
    create index IF NOT EXISTS for (n:ID) on (n.name,n.type);
    """
    cypher_index2 = """
    create index IF NOT EXISTS for ()-[r:Relation]-() on (r.name,r.type);
    """
    cypher_count = f'''
        LOAD CSV WITH HEADERS FROM 'file:///{the_file}' AS row
        RETURN count(row);
    '''
    cypher_node_from = f'''
        USING PERIODIC COMMIT 5000
        LOAD CSV WITH HEADERS FROM 'file:///{the_file}' AS row
        MERGE (from_id:ID {{name: row.from, type: row.from_type}})
        on create set from_id.name=row.from,from_id.type=row.from_type
        RETURN count(from_id);
    '''
    cypher_node_to = f'''
        USING PERIODIC COMMIT 5000
        LOAD CSV WITH HEADERS FROM 'file:///{the_file}' AS row
        MERGE (to_id:ID {{name: row.to, type: row.to_type}})
        on create set to_id.name=row.to,to_id.type=row.to_type
        RETURN count(to_id);
    '''
    cypher_relation = f'''
        USING PERIODIC COMMIT 5000
        LOAD CSV WITH HEADERS FROM 'file:///{the_file}' AS row
        match (from_id:ID {{name: row.from, type: row.from_type}})
        match (to_id:ID {{name: row.to, type: row.to_type}})
        MERGE (from_id)-[rel:Relation {{type:row.link_type}}]->(to_id)
        on create set rel.type= row.link_type
        RETURN count(rel);
    '''
    cypher_remove_self_loop = '''
        MATCH self_loop = (m)-[r]-(n) where m.name = n.name delete r;
    '''
    cypher_count_self_loop = '''
        MATCH self_loop = (m)-[r]-(n) where m.name = n.name return count(r);
    '''

    cypher_apoc_node_label = '''
    // BY id type
        MATCH (n:ID)
        WITH DISTINCT n.type AS id_type, collect(DISTINCT n) AS persons
        CALL apoc.create.addLabels(persons, [apoc.text.upperCamelCase(id_type)]) YIELD node
        RETURN count(node)
    '''

    cypher_apoc_rel_type = '''
        MATCH ()-[rel:Relation]->()
        CALL apoc.refactor.setType(rel, rel.type)
        YIELD input, output
        RETURN count(input)
    '''

    cypher_label_mark = """
        CALL apoc.periodic.iterate(
        "MATCH (p:ID) RETURN p",
        "SET p:ID",
        {batchSize: 5000}
        )
        YIELD batch, operations
        return batch,operations
    """

    cypher_node_list = [
        cypher_conf,
        cypher_clean,
        cypher_constraint_id_from_to,
        cypher_index1,
        cypher_index2,
        cypher_count,
        cypher_node_from,
        cypher_node_to,
        cypher_relation,
        cypher_remove_self_loop,
        cypher_count_self_loop,
        cypher_apoc_node_label,
        cypher_apoc_rel_type,
        # cypher_label_mark
    ]
    return cypher_node_list