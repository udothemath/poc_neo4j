# %%
import os
from dataclasses import dataclass
from src.utils import create_csv_file, logger_decorator, create_logger, log_info
from src.neo4j_conn import Neo4jConnection
from setting import NEO4J_PASSWORD, NEO4J_USER
from src.cypher_code import (cypher_clean, cypher_conf)
from datetime import datetime
import logging

PATH_BOLT = "bolt://localhost:7687"
# MAIN_PATH = "/Users/pro/Documents/poc_neo4j/" ## pro
MAIN_PATH = "/home/jovyan/poc_neo4j/"  # aicloud
DATA_PATH = os.path.join(MAIN_PATH, 'data')
# dir_path = os.path.abspath(os.getcwd()) + "/"
LOG_FILENAME = f"log_{datetime.now():%Y%m%d}.log"
the_logger = create_logger(
    LOG_FILENAME, 'log', MAIN_PATH, turn_on_console=False)


@dataclass
class FileInfo:
    file_prefix: str
    num_rows: int
    type_size: int
    file_path: str

    @property
    def filename_path(self):
        return os.path.join(self.file_path, f'{self.file_prefix}_size{self.num_rows}.csv')


class RunNeo4jFile:
    def __init__(self, file_source, logger: logging):
        self.logger = logger
        self.file_source = file_source
        self.filename_path = self._check_file()

    @logger_decorator(the_logger)
    def _check_file(self) -> None:
        if isinstance(self.file_source, str):
            if os.path.isfile(self.file_source):
                print("load the existing data from data folder")
                return self.file_source
            else:
                raise ValueError("data isn't ready.")
        elif isinstance(self.file_source, FileInfo):
            filename_path = self.file_source.filename_path
            if os.path.isfile(filename_path):
                print("u already have csv file. Do nothing...")
            else:
                try:
                    create_csv_file(self.file_source)
                    print("u have created csv file.")
                except:
                    raise ValueError("Have trouble create files")
            return filename_path
        else:
            raise ValueError("Check your data...")

    @logger_decorator(the_logger)
    def main(self):
        the_file = self.filename_path
        print(" ------------- ")
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
            MERGE (from_id)-[rel:Relation {{type:row.link_type}}]->(to_id)
            on create set rel.type= row.link_type
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
            print("hello")
            for i, run_cypher in enumerate(cypher_list, start=1):
                print(f"Current cypher: {i:02}/{total_n_of_cypher}")
                try:
                    curr_job = driver.query(run_cypher)
                    print(f"{curr_job}")
                except:
                    raise ValueError("something goes wrong...")


def run_real_data():
    DATA_PATH = "/home/jovyan/socialnetwork_info_TFS/go_neo4j/data"
    FILE_CSV = "a_20230617_all_links_size2000000.csv"

    the_filename = os.path.join(DATA_PATH, FILE_CSV)
    a = RunNeo4jFile(the_filename, the_logger)
    a.main()


def run_toy_data():
    DATA_PATH = "/home/jovyan/socialnetwork_info_TFS/poc_neo4j/data"
    the_csv = FileInfo('sample', 100, 5, DATA_PATH)
    b = RunNeo4jFile(the_csv, the_logger)
    b.main()

if __name__ == "__main__":
    print("---run main---")
    print(f"Check your current directory: {MAIN_PATH}")
    run_toy_data()
    print("---done in main---")




