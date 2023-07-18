# %%
import os
from dataclasses import dataclass
from src.utils import create_csv_file, logger_decorator, create_logger, log_info
from src.neo4j_conn import Neo4jConnection
from setting import NEO4J_PASSWORD, NEO4J_USER
from src.cypher_code import (cypher_clean, cypher_conf)
from datetime import datetime

PATH_BOLT = "bolt://localhost:7687"
MAIN_PATH = "/Users/pro/Documents/poc_neo4j/"
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

        cypher_constraint_from = f'''
            CREATE CONSTRAINT IF NOT EXISTS ON (m:FROM_ID) ASSERT m.from IS UNIQUE
        '''

        cypher_constraint_to = f'''
            CREATE CONSTRAINT IF NOT EXISTS ON (m:TO_ID) ASSERT m.to IS UNIQUE
        '''

        cypher_count = f'''
            LOAD CSV WITH HEADERS FROM 'file:///{the_file}' AS row
            RETURN count(row);
        '''

        cypher_from_rel_to = f'''
            USING PERIODIC COMMIT 5000
            LOAD CSV WITH HEADERS FROM 'file:///{the_file}' AS row
            MERGE (from_id:ID:FROM_ID {{name: row.from, type: row.from_type}})
            MERGE (to_id:ID:TO_ID {{name: row.to, type: row.to_type}})
            MERGE (from_id) - [rel:relation {{type: row.relation}}] -> (to_id)       
            RETURN count(from_id), count(to_id), count(rel);
        '''

        with Neo4jConnection(uri=PATH_BOLT, user=NEO4J_USER, pwd=NEO4J_PASSWORD) as driver:
            print(driver.query(cypher_clean))
            print(driver.query(cypher_constraint_from))
            print(driver.query(cypher_constraint_to))
            print(driver.query(cypher_conf))
            print(driver.query(cypher_count))
            print(driver.query(cypher_from_rel_to))


if __name__ == "__main__":
    print("---run main---")
    print(f"Check your current directory: {MAIN_PATH}")
    # Run for loop for data loading check
    # for i in [10, 100, 1_000, 10_000, 100_000, 1_000_000, 10_000_000]:
    for i in [10, 100, 1_000]:
        the_csv = FileInfo('sample', i, 5, DATA_PATH)
        go = RunNeo4j(the_csv, the_logger)
        go.main()
    print("---done in main---")
# %%
