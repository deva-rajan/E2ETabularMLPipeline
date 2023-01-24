"""
This is a boilerplate pipeline 'feature_engineering'
generated using Kedro 0.18.4
"""

from kedro.pipeline import Pipeline, node, pipeline

from e2etabularmlpipeline.pipelines.feature_engineering.nodes import encoding


def create_pipeline(**kwargs) -> Pipeline:
    auto_feateng_list = [encoding, scaling]
    #auto_feateng_list = [encoding]

    node_list = []
    for index in range(len(auto_feateng_list)):
        cur_func = auto_feateng_list[index]
        if index == 0:
            node_feateng = node(func=cur_func, inputs=["auto_feateng", "params:data.auto_feateng",
                                                       "params:feature_engineering.encoding"],
                                outputs=f"output_{cur_func.__name__}", name=str(cur_func.__name__), tags=str(cur_func.__name__))
        else:
            prev_step = auto_feateng_list[index - 1]
            node_feateng = node(func=cur_func, inputs=[f"output_{str(prev_step.__name__)}", "params:data.auto_feateng",
                                                       "params:feature_engineering"],
                                outputs=f"output_{str(cur_func.__name__)}", name=str(cur_func.__name__))

        node_list.append(node_feateng)

    return pipeline(node_list)
