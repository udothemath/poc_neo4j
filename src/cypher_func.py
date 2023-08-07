cypher_clean = '''
MATCH (n) DETACH DELETE n
'''
cypher_conf = '''
CALL dbms.listConfig()
YIELD name, value
WHERE name STARTS WITH 'dbms.default'
RETURN name, value
ORDER BY name
LIMIT 3;
'''


def cypher_property_code(the_file: str):
    cypher_constraint_node_id = '''
        CREATE CONSTRAINT IF NOT EXISTS ON (m:ID) ASSERT (m.id_number) IS UNIQUE
    '''
    cypher_index_node_id = """
        CREATE INDEX node_index_id_number IF NOT EXISTS FOR (n:ID) ON (n.id_number)
    """

    # cypher_property1 = f'''
    #     USING PERIODIC COMMIT 5000
    #     LOAD CSV WITH HEADERS FROM 'file:///{the_file}' AS row
    #     MERGE (id:ID {{id_number: row.node_id}} )
    #     RETURN count(id);
    # '''

    # cypher_property2 = f'''
    #     USING PERIODIC COMMIT 5000
    #     LOAD CSV WITH HEADERS FROM 'file:///{the_file}' AS row
    #     MATCH (id:ID {{id_number: row.node_id}} )
    #     SET
    #     id.name=row.name,
    #     id.birthdate=row.birthdate,
    #     id.sex=row.sex,
    #     id.registered_address=row.registered_address
    #     RETURN count(id);
    # '''

    cypher_apoc_iterate_property_merge = f"""
        CALL apoc.periodic.iterate(
        "LOAD CSV WITH HEADERS FROM 'file:///{the_file}' AS row RETURN row",
        "MERGE (id:ID {{id_number: row.node_id}} )",
            {{batchSize: 5000}}
        )
        YIELD batch, operations
        return batch, operations
    """

    cypher_apoc_iterate_property_set = f"""
        CALL apoc.periodic.iterate(
        "LOAD CSV WITH HEADERS FROM 'file:///{the_file}' AS row MATCH (id:ID {{id_number: row.node_id}} ) RETURN id,row",
        "SET id.sex=row.sex, id.registered_address=row.registered_address",
            {{batchSize: 5000}}
        )
        YIELD batch, operations
        return batch, operations
    """

    cypher_list = [
        cypher_constraint_node_id,
        cypher_index_node_id,
        # cypher_property1,
        # cypher_property2
        cypher_apoc_iterate_property_merge,
        cypher_apoc_iterate_property_set
    ]
    return cypher_list


def cypher_node_code(the_file: str):

    cypher_constraint_id_from_to = '''
        CREATE CONSTRAINT IF NOT EXISTS ON (m:ID) ASSERT m.id_number IS UNIQUE
    '''
    cypher_index1 = """
    create index IF NOT EXISTS for (n:ID) on n.id_number;
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
        MERGE (from_id:ID {{id_number: row.from, type: row.from_type}})
        on create set from_id.id_number=row.from,from_id.type=row.from_type
        RETURN count(from_id);
    '''
    cypher_node_to = f'''
        USING PERIODIC COMMIT 5000
        LOAD CSV WITH HEADERS FROM 'file:///{the_file}' AS row
        MERGE (to_id:ID {{id_number: row.to, type: row.to_type}})
        on create set to_id.id_number=row.to,to_id.type=row.to_type
        RETURN count(to_id);
    '''
    cypher_relation = f'''
        USING PERIODIC COMMIT 5000
        LOAD CSV WITH HEADERS FROM 'file:///{the_file}' AS row
        match (from_id:ID {{id_number: row.from, type: row.from_type}})
        match (to_id:ID {{id_number: row.to, type: row.to_type}})
        MERGE (from_id)-[rel:Relation {{type:row.link_type}}]->(to_id)
        on create set rel.type= row.link_type
        RETURN count(rel);
    '''
    cypher_remove_self_loop = '''
        MATCH self_loop = (m)-[r]-(n) where m.id_number = n.id_number delete r;
    '''
    cypher_count_self_loop = '''
        MATCH self_loop = (m)-[r]-(n) where m.id_number = n.id_number return count(r);
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

    # cypher_label_mark = """
    #     CALL apoc.periodic.iterate(
    #     "MATCH (p:ID) RETURN p",
    #     "SET p:ID",
    #     {batchSize: 5000}
    #     )
    #     YIELD batch, operations
    #     return batch,operations
    # """

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
