# %%
import os
from dataclasses import dataclass
from src.utils import create_csv_file, logger_decorator, create_logger, log_info
from src.neo4j_conn import Neo4jConnection
from setting import NEO4J_PASSWORD, NEO4J_USER
from src.cypher_code import (cypher_clean, cypher_conf)
from datetime import datetime
import logging
from src.generate_csv import FileInfoFromDB, GenCSVfromDB
# pd.set_option('display.max_columns', 9999)

PATH_BOLT = "bolt://localhost:7687"
# MAIN_PATH = "/Users/pro/Documents/poc_neo4j/" ## pro
# MAIN_PATH = "/home/jovyan/poc_neo4j/"  # aicloud
MAIN_PATH = os.path.abspath(os.getcwd()) + "/"
DATA_PATH = os.path.join(MAIN_PATH, 'data')
LOG_FILENAME = f"log_{datetime.now():%Y%m%d}.log"
THE_LOGGER = create_logger(
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


class RunNeo4jFile:
    def __init__(self, file_source, logger: logging):
        self.logger = logger
        self.file_source = file_source
        self.filename_path = self._check_file()

    @logger_decorator(THE_LOGGER)
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

    @logger_decorator(THE_LOGGER)
    def main(self):
        the_file = self.filename_path
        print(" ------------- ")
        # print(split_large_file(the_file,5000))
        cypher_constraint_id_from = '''
            CREATE CONSTRAINT IF NOT EXISTS ON (m:ID) ASSERT m.from IS UNIQUE
        '''
        cypher_constraint_id_to = '''
            CREATE CONSTRAINT IF NOT EXISTS ON (m:ID) ASSERT m.to IS UNIQUE
        '''
        cypher_constraint_from = '''
            CREATE CONSTRAINT IF NOT EXISTS ON (m:FROM_ID) ASSERT m.from IS UNIQUE
        '''
        cypher_constraint_to = '''
            CREATE CONSTRAINT IF NOT EXISTS ON (m:TO_ID) ASSERT m.to IS UNIQUE
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

        cypher_label_mark = """
            CALL apoc.periodic.iterate(
            "MATCH (p) where p:FROM_ID or p:TO_ID RETURN p",
            "set p:ID",
            {batchSize: 5000}
            )
            YIELD batch, operations
            return batch,operations
        """
        cypher_list = [
            cypher_conf,
            cypher_clean,
            cypher_constraint_id_from,
            cypher_constraint_id_to,
            # cypher_constraint_from,
            # cypher_constraint_to,
            cypher_index1,
            cypher_index2,
            cypher_count,
            cypher_node_from,
            cypher_node_to,
            cypher_relation,
            cypher_remove_self_loop,
            cypher_count_self_loop
            #    cypher_label_mark
        ]
        total_n_of_cypher = len(cypher_list)
        with Neo4jConnection(uri=PATH_BOLT, user=NEO4J_USER, pwd=NEO4J_PASSWORD) as driver:
            for i, run_cypher in enumerate(cypher_list, start=1):
                print(f"Current cypher: {i:02}/{total_n_of_cypher}")
                print(f"Cypher code: {run_cypher}")
                try:
                    curr_job = driver.query(run_cypher)
                    print(f"{curr_job}")
                except:
                    raise ValueError("something goes wrong...")


def create_csv_from_db(file_info_fromDB: FileInfoFromDB, logger=THE_LOGGER):
    """
    Create csv file and save to desired directory
    Args:
        table_info (dataclass): table info
        logger (str): Logging
    Returns:
        None
    """
    file_with_path = file_info_fromDB.get_path
    if os.path.isfile(file_with_path):
        print(f"u already have file: {file_with_path}")
    else:
        print("file doesn't exists. Creating file...")
        GenCSVfromDB(file_info_fromDB, logger=logger).create_csv_from_df()
        print(f"file is ready. {file_with_path}")


def run_toy_data(data_path: str, logger: logging):
    the_csv = FileInfo('sample', 100, 5, data_path)
    b = RunNeo4jFile(the_csv, logger)
    b.main()


def run_real_data(data_path: str, file_csv: str, logger: logging):
    the_filename = os.path.join(data_path, file_csv)
    a = RunNeo4jFile(the_filename, logger)
    a.main()


if __name__ == "__main__":
    print("---run main---")
    print(f"Check your current directory: {MAIN_PATH}")

    # DATA_PATH = "/home/jovyan/socialnetwork_info_TFS/poc_neo4j/data"
    # run_toy_data(data_path=DATA_PATH, logger=THE_LOGGER)

    # generate data from feature DB
    # DATA_PATH = "/home/jovyan/socialnetwork_info_TFS/poc_neo4j/data"
    # file_all_links = FileInfoFromDB(table_name="all_links", save_dir=DATA_PATH,
    #                   save_file_prefix="20230721_v2", size_limit=40)
    # create_csv_from_db(file_all_links)

    # FILE_CSV = "20230720_all_links_size30.csv"
    # # FILE_CSV = "20230720_v2_all_links.csv"
    # run_real_data(data_path=DATA_PATH,
    #               file_csv=FILE_CSV,
    #               logger=THE_LOGGER)
    print("---done in main---")
