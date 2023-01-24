from .BaseFeatureEngineering import BaseFeatureEngineering
import modin.pandas as pd
import logging
from sklearn.preprocessing import OneHotEncoder, MinMaxScaler, MaxAbsScaler, RobustScaler
from statsmodels.stats.outliers_influence import variance_inflation_factor
from sklearn.preprocessing import StandardScaler


# class Imputation(BaseFeatureEngineering):
#
#     def process(self):
#         pass


# class Variance(BaseFeatureEngineering):
#     pass

class Encoding(BaseFeatureEngineering):

    def __init__(self, df, data_config, process_config):
        super().__init__(df, data_config, process_config)

    def process(self):
        logger = logging.getLogger(__name__)
        BaseFeatureEngineering.print_stats(self.rawdf)
        logger.info(self.process_config)
        self.infer_datatypes(self.rawdf)

        workingdf = self.remove_id_target_cols(self.rawdf)
        workingdf = self.get_cols(['object'], workingdf)

        df_obj_columns = workingdf.select_dtypes(include=['object'])

        if self.process_config['method'] == "onehot":
            ohe = OneHotEncoder(dtype='int32')
            ohe_matrix = ohe.fit_transform(df_obj_columns)
            ohe_df = pd.DataFrame(data=ohe_matrix.toarray(), columns=ohe.get_feature_names_out())

            # Drop non-business explanatory columns
            nan_cols = [col for col in ohe_df.columns if col.endswith('_nan')]
            ohe_df = ohe_df.drop(columns=nan_cols, axis=1)
            finaldf = self.add_cols(['numeric', 'datetime', 'bool'], ohe_df)
            finaldf = self.add_id_target_cols(finaldf)

        self.finaldf = finaldf
        BaseFeatureEngineering.print_stats(finaldf)
        BaseFeatureEngineering.reconcile_data(self.rawdf, self.finaldf)
        self.calc_exec_time()


'''
Normalization and Standardization
'''


class Scaling(BaseFeatureEngineering):
    rawdf = None
    finaldf = None
    df_config = None
    scaler_type = None

    transformers = {
        'standard': StandardScaler(),
        'minmax': MinMaxScaler(),
        'maxabs': MaxAbsScaler(),
        'robust': RobustScaler()
    }

    def __init__(self, df, df_config, scaler_type, scaler_func='fit_transform', **scaler_params):
        super().__init__()
        self.rawdf = df.copy(deep=True)
        self.data_config = df_config
        self.scaler_type = scaler_type
        self.scaler_func = scaler_func
        self.scaler_params = scaler_params

    def process(self):
        BaseFeatureEngineering.print_stats(self.rawdf)
        workingdf = self.get_cols(['numeric'], remove_id_target=True)
        scaler = self.transformers[self.scaler_type]
        scaler.set_params(self.scaler_params)
        if self.scaler_func == "fit":
            scaler.fit(workingdf)
        elif self.scaler_func == "fit_transform":
            finaldf = scaler.fit_transform(workingdf)
        elif self.scaler_func == "transform":
            finaldf = scaler.transform(workingdf)
        finaldf = self.add_cols(['bool', 'object', 'datetime'], finaldf)
        self.finaldf = finaldf


def handle_using_vif(df, threshold=10):
    cols_removed = []
    non_linear_cols = []
    non_linear_cols_counter = 1  # initializing to non-zero for at least one round of check
    vifdf = df.copy(deep=True)
    while non_linear_cols_counter > 0:
        colnames = vifdf.columns
        vif_output = [variance_inflation_factor(vifdf.values, i) for i in range(vifdf.shape[1])]
        non_collinear_cols = [colnames[idx] for idx, vif_val in enumerate(vif_output) if vif_val > threshold]
        vifdf = vifdf.drop(non_linear_cols, axis=1)
        cols_removed.append(non_collinear_cols)
        non_linear_cols_counter = len(non_collinear_cols)

    finaldf = df.drop(cols_removed, axis=1)

    return finaldf


class MultiCollinearity(BaseFeatureEngineering):

    def __init__(self, df, df_config, technique='vif'):
        super().__init__()
        self.rawdf = df.copy(deep=True)
        self.dfconfig = df_config
        if technique not in ['vif', 'correlation']:
            raise ValueError('Please make sure the entered value is vif or correlation')
        else:
            self.techique = technique

    def handle_using_correlation(df, threshold=0.8):
        corrdf = df.corr().abs().unstack().sort_values(ascending=False)
        corrdf = corrdf.reset_index()
        corrdf.columns = ['col1', 'col2', 'corr_val']
        correlated_df = corrdf[(corrdf['col1'] != corrdf['col2']) & (corrdf['corr_val'] >= threshold)]
        cols_to_remove = list(set(correlated_df.col1))
        return df.drop(cols_to_remove, axis=1)

    def process(self):
        workingdf = self.get_cols(self.rawdf, ['numeric'], exclude_id_target=True)

        if self.techique == "vif":
            finaldf = handle_using_vif(workingdf)
        elif self.techique == "correlation":
            finaldf = self.handle_using_correlation(workingdf)

        self.finaldf = self.add_cols(['bool', 'object', 'datetime'], finaldf)
