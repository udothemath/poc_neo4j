from neo4j import GraphDatabase


class Neo4jConnection:
    '''
    neo4j connection
    '''

    def __init__(self, uri: str, user: str, pwd: str):
        self.__uri = uri
        self.__user = user
        self.__pwd = pwd
        self.__driver = None
        try:
            self.__driver = GraphDatabase.driver(
                self.__uri, auth=(self.__user, self.__pwd))
            print("Driver exists")
        except Exception as e:
            print("Failed to create the driver:", e)

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self._close()

    def _check_conn(self):
        cypher_info_check = '''
        CALL dbms.listConnections() YIELD connectionId, connectTime, connector, username, userAgent, clientAddress
        '''
        return self.query(cypher_info_check)

    def _kill_conn(self, list_of_conn_id=['bolt-386', 'bolt-394', 'bolt-384']):
        cypher_conn_id='CALL dbms.listConnections() YIELD connectionId'
        print(self.query(cypher_conn_id))
        # curr_conn_id=self.query()
        cypher_kill=f'''
        CALL dbms.killConnections({list_of_conn_id})
        '''
        return self.query(cypher_kill)

    def _close(self):
        if self.__driver is not None:
            self.__driver.close()

    def query(self, query, parameters=None, db='neo4j'):
        assert self.__driver is not None, "Driver not initialized!"
        session = None
        response = None
        try:
            session = self.__driver.session(
                database=db) if db is not None else self.__driver.session()
            response = list(session.run(query, parameters))
        except Exception as e:
            print("Query failed:", e)
        finally:
            if session is not None:
                session.close()
        return response
