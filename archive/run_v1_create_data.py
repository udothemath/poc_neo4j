# %%

import numpy as np
import pandas as pd
from neo4j import GraphDatabase
from tqdm import tqdm

from setting import NEO4J_PASSWORD, NEO4J_USER
from src.cypher_code import (cypher_clean, cypher_conf,
                             cypher_csv_cnt_from_pro, cypher_csv_cnt_import,
                             cypher_csv_create_from_pro, cypher_html_csv,
                             cypher_info, cypher_node, load_csv_as_row)
from src.neo4j_conn import Neo4jConnection

PATH_BOLT = "bolt://localhost:7687"
data_source_path = '/Users/pro/Downloads/real_estate.csv'


def read_csv_as_chunk(fname, sample_size, chunk_size=1000):
    reader = pd.read_csv(fname, header=0, nrows=sample_size,
                         iterator=True, low_memory=False)
    chunks = []
    loop = True
    while loop:
        try:
            chunk = reader.get_chunk(chunk_size)
            chunks.append(chunk)
        except StopIteration:
            loop = False
            print("Iteration is stopped")

    df_ac = pd.concat(chunks, ignore_index=True)
    return df_ac


def data_csv_process(sample_size):
    to_path = '/Users/pro/Documents/ml_with_graph_algorithms/nice_graph/neo4j_go/data/'
    df = read_csv_as_chunk(data_source_path, sample_size, chunk_size=10000)
    print(df.columns)
    print(df.shape)
    col_name = ['交易標的',
                '土地位置/建物門牌',
                '土地移轉總面積(坪)',
                '交易年月日',
                '建築完成年月',
                '建物移轉總面積(坪)',
                '建物現況格局-房',
                '建物現況格局-廳',
                '建物現況格局-衛',
                '建物現況格局-隔間',
                '總價(元)',
                '車位類別',
                '車位移轉總面積(坪)',
                '車位總價(元)',
                '編號',
                'house_age',
                'city_nm2',
                '鄉鎮市區',
                'x座標',
                'y座標',
                ]
    df = df[col_name]
    df.to_csv(f"{to_path}/data_sub_size{sample_size}", index=False)


def main():
    with Neo4jConnection(uri=PATH_BOLT, user=NEO4J_USER, pwd=NEO4J_PASSWORD) as driver:
        print(driver.query(cypher_conf))
        print(driver.query(cypher_csv_cnt_from_pro))
        # print(driver.query(cypher_html_csv))


if __name__ == "__main__":
    print(f"{'-'*20}")
    main()
    # data_csv_process(sample_size=1000)
    print(f"{'-'*20}")
