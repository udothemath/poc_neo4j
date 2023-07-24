# %%
import os
from dataclasses import dataclass
from src.utils import (create_csv_file, logger_decorator,
                       create_logger, log_info, create_csv_file_property)
from src.neo4j_conn import Neo4jConnection
from setting import NEO4J_PASSWORD, NEO4J_USER
from src.cypher_code import (cypher_clean, cypher_conf)
from datetime import datetime
import logging
from src.generate_csv import FileInfoFromDB, GenCSVfromDB
from src.cypher_func import cypher_node_code, cypher_property_code
# pd.set_option('display.max_columns', 9999)

PATH_BOLT = "bolt://localhost:7687"
MAIN_PATH = os.path.abspath(os.getcwd()) + "/"
DATA_PATH = os.path.join(MAIN_PATH, 'data')
if not os.path.exists(DATA_PATH):
    os.makedirs(DATA_PATH)
LOG_FILENAME = f"log_{datetime.now():%Y%m%d}.log"
THE_LOGGER = create_logger(
    LOG_FILENAME, 'log', MAIN_PATH, turn_on_console=True)


@dataclass
class FileInfo:
    file_prefix: str
    num_rows: int
    type_size: int
    file_path: str = DATA_PATH

    @property
    def filename_path(self):
        return os.path.join(self.file_path, f'{self.file_prefix}_size{self.num_rows}.csv')


@dataclass
class PropertyInfo:
    file_prefix: str
    num_rows: int
    file_path: str = DATA_PATH

    @property
    def filename_path(self):
        return os.path.join(self.file_path, f'{self.file_prefix}_size{self.num_rows}.csv')


class RunNeo4jFile:
    logger = THE_LOGGER

    def __init__(self, file_source):
        self.file_source = file_source
        self.filename_path = self._check_file()

    @logger_decorator(logger)
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

    @logger_decorator(logger)
    def neo4j_execute_cypher(self, func):
        the_file = self.filename_path
        cypher_node_list = func(the_file)
        print(" ------------- ")
        total_n_of_cypher = len(cypher_node_list)
        with Neo4jConnection(uri=PATH_BOLT, user=NEO4J_USER, pwd=NEO4J_PASSWORD) as driver:
            for i, run_cypher in enumerate(cypher_node_list, start=1):
                print(f"Current cypher: {i:02}/{total_n_of_cypher}")
                print(f"Cypher code: {run_cypher}")
                try:
                    curr_job = driver.query(run_cypher)
                    print(f"{curr_job}")
                except:
                    raise ValueError("something goes wrong...")


def create_csv_from_db(file_info_fromDB: FileInfoFromDB, logger=logging.getLogger(__name__)):
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
        try:
            GenCSVfromDB(file_info_fromDB, logger=logger).create_csv_from_df()
            print(f"file is ready. {file_with_path}")
        except:
            raise ValueError("Cannot generate csv from DB")
    return file_with_path


if __name__ == "__main__":
    print("---run main---")
    print(f"Check your current directory: {MAIN_PATH}")
    print(f"Check your data directory: {DATA_PATH}")

    # generate data
    data_row_size = 10_000
    data_node = FileInfo('demo', data_row_size, 5, file_path=DATA_PATH)
    file_csv_node = create_csv_file(data_node)

    data_property = PropertyInfo(
        'property', data_row_size, file_path=DATA_PATH)
    file_csv_property = create_csv_file_property(data_property)
    print(f"node file: {file_csv_node}")
    print(f"property file: {file_csv_property}")

    RunNeo4jFile(file_csv_node).neo4j_execute_cypher(cypher_node_code)
    RunNeo4jFile(file_csv_property).neo4j_execute_cypher(cypher_property_code)


    # generate data from feature DB
    # data_from_db = FileInfoFromDB(table_name="l3_node_person", save_dir=DATA_PATH,
    #                   save_file_prefix="20230724_v1", size_limit=1_000)
    # file_csv = create_csv_from_db(data_from_db)
    # RunNeo4jFile(file_csv).main()

    # generate data for property
    # data_from_db_property = FileInfoFromDB(table_name="l3_node_person", save_dir=DATA_PATH,
    #                   save_file_prefix="20230724_v1", size_limit=1_000)
    # file_csv_property = create_csv_from_db(data_from_db_property)
    # # file_csv = '/home/jovyan/socialnetwork_info_TFS/poc_neo4j/data/20230724_v1_l3_node_person_size10.csv'
    # RunNeo4jFile(file_csv_property).add_property(file_property=file_csv_property)

    print("---done in main---")
