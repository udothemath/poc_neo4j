# %%
import os
from dataclasses import dataclass
from src.utils import (create_csv_file_node, logger_decorator,
                       create_logger, create_csv_file_property)
from src.neo4j_conn import Neo4jConnection
from setting import NEO4J_PASSWORD, NEO4J_USER
from datetime import datetime
from src.generate_csv import FileInfoFromDB, GenCSVfromDB
from src.cypher_func import cypher_node_code, cypher_property_code
# pd.set_option('display.max_columns', 9999)

PATH_BOLT = "bolt://localhost:7687"
MAIN_PATH = os.path.abspath(os.getcwd()) + "/"
DATA_PATH = os.path.join(MAIN_PATH, 'data')
DATA_PATH = "/home/jovyan/socialnetwork-info/neo4j_demo/poc_neo4j/data"
if not os.path.exists(DATA_PATH):
    os.makedirs(DATA_PATH)
LOG_FILENAME = f"log_{datetime.now():%Y%m%d}.log"
THE_LOGGER = create_logger(
    LOG_FILENAME, 'log', MAIN_PATH, turn_on_console=True)


@dataclass
class FileInfo:
    """
        Define file info
    """
    file_prefix: str
    num_rows: int
    type_size: int = 0
    file_path: str = DATA_PATH

    @property
    def filename_path(self):
        return os.path.join(self.file_path, f'{self.file_prefix}_size{self.num_rows}.csv')


class RunNeo4jFile:
    """
        Execute cypher code in neo4j
    """
    logger = THE_LOGGER

    def __init__(self, file_source, logger=THE_LOGGER):
        self.file_source = file_source
        self._check_file()
        self.logger = logger

    @logger_decorator(logger)
    def _check_file(self) -> None:
        """
            Check whether file exists
        """
        try:
            assert os.path.isfile(self.file_source)
            print(f"file {self.file_source} exists.")
        except:
            self.logger.error(f"file {self.file_source} doesn't exist.")
            raise

    @logger_decorator(logger)
    def neo4j_execute_cypher(self, func):
        """
            Execute cypher code
        """
        cypher_node_list = func(self.file_source)
        print(" ------------- ")
        total_n_of_cypher = len(cypher_node_list)
        with Neo4jConnection(uri=PATH_BOLT, user=NEO4J_USER, pwd=NEO4J_PASSWORD) as driver:
            for i, run_cypher in enumerate(cypher_node_list, start=1):
                print(f"Current cypher: {i:02}/{total_n_of_cypher}")
                print(f"Cypher code: {run_cypher}")
                try:
                    curr_job = driver.query(run_cypher)
                    print(f"{curr_job}")
                except BaseException as e:
                    logger.info(
                        '[error] cypher execution failed', exc_info=True)
                    raise e


def run_neo4j_demo(data_row_size=1_000, file_path=DATA_PATH):
    # ## generate demo data
    print("--- generate demo data --- ")
    data_node = FileInfo('demo', data_row_size, 5, file_path=file_path)
    file_csv_node = create_csv_file_node(data_node)

    data_property = FileInfo(
        'property', data_row_size, file_path=file_path)
    file_csv_property = create_csv_file_property(data_property)
    print(f"demo node file: {file_csv_node}")
    print(f"demo property file: {file_csv_property}")
    RunNeo4jFile(file_csv_node).neo4j_execute_cypher(cypher_node_code)
    RunNeo4jFile(file_csv_property).neo4j_execute_cypher(cypher_property_code)
    print("--- end: generate demo data --- ")


def run_neo4j():
    # ##
    # Note: 3 steps to insert data into neo4j
    # step1: Define data info
    # step2: Create CSV from demo setting or db
    # step3: Insert data by generated csv files
    print("---run neo4j---")
    # ## Run demo data
    RUN_DEMO = False
    if RUN_DEMO:
        run_neo4j_demo(data_row_size=100)

    # ## Run db data
    RUN_NEO4J_NODE = True
    RUN_NEO4J_PROP = False
    # ## generate node data from feature DB
    if RUN_NEO4J_NODE:
        data_from_db_node = FileInfoFromDB(table_name="all_links", save_dir=DATA_PATH,
                                           save_file_prefix='20230807_v1', size_limit=1_000)
        file_csv_node = GenCSVfromDB(
            data_from_db_node, logger=THE_LOGGER).filename_with_path()
        RunNeo4jFile(file_csv_node).neo4j_execute_cypher(cypher_node_code)

    # ## generate property data from feature DB
    if RUN_NEO4J_PROP:
        data_from_db_property = FileInfoFromDB(table_name="l3_node_person", save_dir=DATA_PATH,
                                               save_file_prefix="20230807_v1", size_limit=None)
        file_csv_property = GenCSVfromDB(
            data_from_db_property, logger=THE_LOGGER).filename_with_path()
        RunNeo4jFile(file_csv_property).neo4j_execute_cypher(
            cypher_property_code)

    print("---done in run neo4j---")


if __name__ == "__main__":
    print(f"Check your current directory: {MAIN_PATH}")
    print(f"Check your data directory: {DATA_PATH}")
    run_neo4j()
