"""
This is a boilerplate pipeline 'feature_engineering'
generated using Kedro 0.18.4
"""
#from e2etabularmlpipeline.pipelines.feature_engineering import processor_factory
from e2etabularmlpipeline.Util import get_pandas

from e2etabularmlpipeline.pipelines.feature_engineering.auto_feateng import Encoding
from e2etabularmlpipeline import Util
import modin.pandas as pd


def encoding(auto_feateng: pd.DataFrame,
              data_config: dict, process_config: dict):
    encoder = Encoding(auto_feateng, data_config, process_config)
    encoder.process()
    return encoder.finaldf


def scaling(
              data_config: dict, process_config: dict):
    encoder = Encoding(auto_feateng, data_config, process_config)
    encoder.process()
    return encoder.finaldf