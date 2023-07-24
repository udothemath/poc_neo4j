from dataclasses import dataclass
import os
import csv
import random
import math


@dataclass
class FileInfoFromDB:
    '''
        FileInfoFromDB
    '''
    table_name: str
    save_dir: str = './'
    save_file_prefix: str = ''
    size_limit: int = 20

    @property
    def get_path(self) -> str:
        if self.size_limit:
            return os.path.join(self.save_dir,
                                f"{self.save_file_prefix}_{self.table_name}_size{self.size_limit}.csv")
        else:
            return os.path.join(self.save_dir,
                                f"{self.save_file_prefix}_{self.table_name}.csv")


class GenCSVfromDB:
    def __init__(self, file_info: FileInfoFromDB, logger):
        '''
            Generate csv from db using easy_to_sql
        '''
        from easy_to_sql.sql_tools import SQLTools
        self._proj_name = 'socialnetwork_info'
        self._file_info = file_info
        self._logger = logger
        self._sq = SQLTools(proj_name=self._proj_name,
                            logger=self._logger, run_by_airflow=False)

    def get_db_df(self):
        '''
            get dataframe from db
        '''
        select_sql = f'''
        SELECT *
        FROM {self._proj_name}.{self._file_info.table_name}
        -- WHERE from_type = 'person'
        -- AND to_type = 'person'
        -- AND link_type NOT IN ('has_grandparents_in_law', 'has_maternal_grandparents', 'has_parents_in_law', 'has_paternal_grandparents')
        '''
        if self._file_info.size_limit:
            size_limit = self._file_info.size_limit
            select_sql = select_sql + f' LIMIT {size_limit}'
        try:
            result = self._sq.query2dataframe(
                select_sql=select_sql,
                db_id='feature',
                output_type='pandas'
            )
            return result
        except:
            raise ValueError("Cannot access db")

    def create_csv_from_df(self):
        '''
            create csv file using pandas dataframe
        '''
        df = self.get_db_df()
        df.to_csv(self._file_info.get_path, sep=',',
                  encoding='utf-8', index=True)
        print(f"filename: {self._file_info.get_path}")


@dataclass
class FileInfo:
    file_prefix: str
    num_rows: int
    file_path: str

    @property
    def filename_path(self):
        return os.path.join(self.file_path, f'{self.file_prefix}_size{self.num_rows}.csv')


def create_csv_file_node(file_info: FileInfo, type_size: int = 5):
    file_prefix = file_info.file_prefix
    num_rows = file_info.num_rows
    file_path = file_info.file_path
    if not os.path.exists(file_path):
        os.makedirs(file_path)
        print(f"You have created folder: {file_path}")
    # Define column names
    fieldnames = ['from', 'to', 'from_type', 'to_type', 'link_type']
    rand_size = int(num_rows/3)
    paddle_zero_size = int(math.log(num_rows, 10))+1

    relation_type_list = [f'type_{i:02}' for i in range(1,  type_size+1)]
    # Generate data for each row
    rows = []
    for i in range(num_rows):
        rand_value_from = random.randint(1, rand_size)
        rand_value_to = random.randint(1, rand_size)

        from_info = random.choice([('a', 'account'),
                                   ('c', 'company'),
                                   ('u', 'user')])
        to_info = random.choice([('a', 'account'),
                                 ('c', 'company'),
                                 ('u', 'user')])

        relation = random.choice(relation_type_list)
        rows.append({'from': f'{from_info[0]}_{rand_value_from:0{paddle_zero_size}}',
                     'to': f'{to_info[0]}_{rand_value_to:0{paddle_zero_size}}',
                     'from_type': from_info[1],
                     'to_type': to_info[1],
                     'link_type': relation})

    # Write data to CSV file
    try:
        filename_path = file_info.filename_path
        with open(filename_path, mode='w', newline='') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        return filename_path
    except:
        raise FileNotFoundError("File not found...")


def create_csv_file_property(file_info: FileInfo):
    file_prefix = file_info.file_prefix
    num_rows = file_info.num_rows
    file_path = file_info.file_path
    if not os.path.exists(file_path):
        os.makedirs(file_path)
        print(f"You have created folder: {file_path}")
    # Define column names
    fieldnames = ['node_id', 'name', 'gender', 'address']
    rand_size = int(num_rows/3)
    paddle_zero_size = int(math.log(num_rows, 10))+1

    # Generate data for each row
    rows = []
    for i in range(num_rows):
        rand_value_from = random.randint(1, rand_size)

        # from_info = random.choice([('a', 'account'),
        #                            ('c', 'company'),
        #                            ('u', 'user')
        #                            ])
        gender_type = random.choice(['M', 'F'])

        rows.append({'node_id': f'a_{rand_value_from:0{paddle_zero_size}}',
                     'name': f'name_{rand_value_from:0{paddle_zero_size}}',
                     'gender': gender_type,
                     'address': f'address_{i:0{paddle_zero_size}}',
                     })

    # Write data to CSV file
    try:
        filename_path = file_info.filename_path
        with open(filename_path, mode='w', newline='') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        return filename_path
    except:
        raise FileNotFoundError("File not found...")
