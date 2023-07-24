from dataclasses import dataclass
import os


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
