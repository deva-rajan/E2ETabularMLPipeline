"""
This is a boilerplate pipeline 'feature_engineering'
generated using Kedro 0.18.4
"""
import logging
from abc import abstractmethod, ABC
from datetime import datetime
import time
import modin.pandas as pd

class BaseFeatureEngineering(ABC):
    rawdf: pd.DataFrame = None
    data_config: dict = {}
    process_config: dict = {}

    start_time: time
    end_time: time

    def __init__(self, df, data_config, process_config):
        self.start_time = time.time()
        self.rawdf = df.copy(deep=True)
        self.data_config = data_config
        self.process_config = process_config

    @staticmethod
    def print_stats(df: pd.DataFrame) -> None:
        logger = logging.getLogger(__name__)
        logger.info(f"""No. of rows {df.shape[0]} and columns {df.shape[1]} in the dataset""")

    @staticmethod
    def reconcile_data(df1: pd.DataFrame, df2: pd.DataFrame) -> None:
        logger = logging.getLogger(__name__)
        logger.info("reconciled data")

    '''
    Input : dataframe to infer datatypes 
    '''


    def infer_datatypes(self, inputdf, update_config=True):
        data_config = self.data_config
        cols_drop = data_config['idcols'] + data_config['target']
        rawdf = inputdf.copy(deep=True)
        df = inputdf.drop(cols_drop, axis=1)
        df_types = df.dtypes.astype(str).to_dict()
        datatype_config = {'numeric': [],
                           'bool': [],
                           'object': [],
                           'datetime': []}

        for key, val in df_types.items():
            if val.startswith('bool'):
                datatype_config['bool'].append(key)
            elif val.startswith('int'):
                datatype_config['numeric'].append(key)
            elif val.startswith('float'):
                datatype_config['numeric'].append(key)
            elif val.startswith('object'):
                datatype_config['object'].append(key)
            elif val.startswith('date'):
                datatype_config['datetime'].append(key)

        df = pd.concat([rawdf[cols_drop], df], axis=1)

        if update_config:
            self.data_config.update(datatype_config)

        return datatype_config

    def add_id_target_cols(self, inputdf: pd.DataFrame) -> pd.DataFrame:
        return pd.concat([self.rawdf[self.data_config['idcols']], inputdf, self.rawdf[self.data_config['target']]], axis=1)

    def remove_id_target_cols(self, inputdf: pd.DataFrame) -> pd.DataFrame:
        cols_remove = self.data_config['idcols'] + self.data_config['target']
        return inputdf.drop(inputdf[cols_remove], axis=1)

    def get_cols(self, col_types: list, inputdf: pd.DataFrame) -> pd.DataFrame:

        df = inputdf.copy(deep=True)

        cols_get = []

        for col_type in col_types:
            if col_type in self.data_config:
                cols_get += self.data_config[col_type]

        cols_get = list(set(cols_get))
        df = df[cols_get]
        return df

    def add_cols(self, col_types: list, df: pd.DataFrame) -> pd.DataFrame:

        rawdf = self.rawdf
        cols_to_add = []

        for col_type in col_types:
            cols = self.data_config[col_type]
            if col_type in self.data_config:
                cols_to_add += self.data_config[col_type]

        finaldf = pd.concat([df, rawdf[cols_to_add]], axis=1)
        return finaldf

    @abstractmethod
    def process(self):
        pass

    def calc_exec_time(self):
        logger = logging.getLogger(__name__)
        self.end_time = time.time()
        hours, rem = divmod(self.end_time - self.start_time, 3600)
        minutes, seconds = divmod(rem, 60)
        logger.info("Total time taken : {:0>2}:{:0>2}:{:05.2f}".format(int(hours), int(minutes), seconds))
