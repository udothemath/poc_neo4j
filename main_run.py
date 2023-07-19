# %%
import os
from dataclasses import dataclass
from src.utils import create_csv_file, logger_decorator, create_logger, log_info
from src.neo4j_conn import Neo4jConnection
from setting import NEO4J_PASSWORD, NEO4J_USER
from src.cypher_code import (cypher_clean, cypher_conf)
from datetime import datetime

PATH_BOLT = "bolt://localhost:7687"
# MAIN_PATH = "/Users/pro/Documents/poc_neo4j/" ## pro
MAIN_PATH = "/home/jovyan/poc_neo4j/"  # aicloud
DATA_PATH = os.path.join(MAIN_PATH, 'data')
# dir_path = os.path.abspath(os.getcwd()) + "/"
LOG_FILENAME = f"log_{datetime.now():%Y%m%d}.log"
the_logger = create_logger(
    LOG_FILENAME, 'log', MAIN_PATH, turn_on_console=True)


@dataclass
class FileInfo:
    file_prefix: str
    num_rows: int
    type_size: int
    file_path: str

    @property
    def filename_path(self):
        return os.path.join(self.file_path, f'{self.file_prefix}_size{self.num_rows}.csv')


class RunNeo4j:
    def __init__(self,  file_info: FileInfo, logger):
        self.file_info = file_info
        self.logger = logger
        self.filename_path = self._check_file()

    @logger_decorator(the_logger)
    def _check_file(self) -> None:
        filename_path = self.file_info.filename_path
        self.logger.info(f"file: {filename_path}")
        self.logger.info(self.file_info)
        if os.path.isfile(filename_path):
            print("u already have csv file. Do nothing...")
        else:
            print("u don't have csv file. Create in progress...")
            create_csv_file(self.file_info)
        return filename_path

    @logger_decorator(the_logger)
    def main(self):
        the_file = self.filename_path
        # print(" ------------->")
        # print(split_large_file(the_file,5000))
        cypher_constraint_from = '''
            CREATE CONSTRAINT IF NOT EXISTS ON (m:FROM_ID) ASSERT m.from IS UNIQUE
        '''
        cypher_constraint_to = '''
            CREATE CONSTRAINT IF NOT EXISTS ON (m:TO_ID) ASSERT m.to IS UNIQUE
        '''
        cypher_index1 = """
        create index IF NOT EXISTS for (n:FROM_ID) on (n.name,n.type);
        """
        cypher_index2 = """
        create index IF NOT EXISTS for (n:TO_ID) on (n.name,n.type);
        """
        cypher_index3 = """
        create index IF NOT EXISTS for ()-[r:Relation]-() on (r.type);
        """

        cypher_count = f'''
            LOAD CSV WITH HEADERS FROM 'file:///{the_file}' AS row
            RETURN count(row);
        '''
        cypher_from_rel_to1 = f'''
            USING PERIODIC COMMIT 5000
            LOAD CSV WITH HEADERS FROM 'file:///{the_file}' AS row
            MERGE (from_id:FROM_ID {{name: row.from, type: row.from_type}})
            on create set from_id.name=row.from,from_id.type=row.from_type
            RETURN count(from_id);
        '''
        cypher_from_rel_to2 = f'''
            USING PERIODIC COMMIT 5000
            LOAD CSV WITH HEADERS FROM 'file:///{the_file}' AS row
            MERGE (to_id:TO_ID {{name: row.to, type: row.to_type}})
            on create set to_id.name=row.to,to_id.type=row.to_type
            RETURN count(to_id);
        '''
        cypher_from_rel_to3 = f'''
            USING PERIODIC COMMIT 5000
            LOAD CSV WITH HEADERS FROM 'file:///{the_file}' AS row
            match (from_id:FROM_ID {{name: row.from, type: row.from_type}})
            match (to_id:TO_ID {{name: row.to, type: row.to_type}})
            MERGE (from_id)-[rel:Relation {{type:row.relation}}]->(to_id)
            on create set rel.type= row.relation
            RETURN count(rel);
        '''
        cypher_label_mark = """
        CALL apoc.periodic.iterate(
        "MATCH (p) where p:FROM_ID or p:TO_ID RETURN p",
        "set p:ID",
        {batchSize: 5000}
        )
        YIELD batch, operations
        return batch,operations
        """
        cypher_list = [cypher_clean,
                       cypher_constraint_from,
                       cypher_constraint_to,
                       cypher_index1,
                       cypher_index2,
                       cypher_index3,
                       cypher_conf,
                       cypher_count,
                       cypher_from_rel_to1,
                       cypher_from_rel_to2,
                       cypher_from_rel_to3,
                       #    cypher_label_mark
                       ]
        total_n_of_cypher = len(cypher_list)
        with Neo4jConnection(uri=PATH_BOLT, user=NEO4J_USER, pwd=NEO4J_PASSWORD) as driver:
            for i, run_cypher in enumerate(cypher_list, start=1):
                print(
                    f"Current cypher: {i:02}/{total_n_of_cypher} {driver.query(run_cypher)}")


if __name__ == "__main__":
    print("---run main---")
    print(f"Check your current directory: {MAIN_PATH}")
    # Run for loop for data loading check
    # for i in [10, 100, 1_000, 10_000, 100_000, 1_000_000, 10_000_000]:
    for i in [10_000_000]:
        the_csv = FileInfo('sample', i, 5, DATA_PATH)
        go = RunNeo4j(the_csv, the_logger)
        go.main()
    print("---done in main---")
# %%
