"""Project pipelines."""
from typing import Dict

from kedro.framework.project import find_pipelines
from kedro.pipeline import Pipeline

from e2etabularmlpipeline.pipelines import data_processing as dp
from e2etabularmlpipeline.pipelines import feature_engineering as fe
from e2etabularmlpipeline.pipelines import modelling as model_train
from e2etabularmlpipeline.pipelines import predict_explain as pred_exp


def register_pipelines() -> Dict[str, Pipeline]:
    """Register the project's pipelines.

    Returns:
        A mapping from pipeline names to ``Pipeline`` objects.
    """

    data_processing_pipeline = dp.create_pipeline()
    feature_engineering_pipeline = fe.create_pipeline()
    model_train_pipeline = model_train.create_pipeline()
    predict_explain_pipeline = pred_exp.create_pipeline()

    # pipelines = find_pipelines()
    # pipelines["__default__"] = sum(pipelines.values())
    pipelines = {"data_processing": data_processing_pipeline,
                 "feature_engineering_pipeline": feature_engineering_pipeline,
                 "model_train_pipeline": model_train_pipeline, "predict_explain_pipeline": predict_explain_pipeline}
    pipelines["__default__"] = sum(pipelines.values())

    return pipelines
