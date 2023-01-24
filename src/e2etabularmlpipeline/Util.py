

from kedro.config import ConfigLoader
import pandas
import modin


def get_parameters(key='all'):
    conf_paths = ["conf/base", "conf/local"]
    conf_loader = ConfigLoader(conf_paths)
    parameters = conf_loader.get("parameters*", "parameters*/**")
    val = parameters if key == 'all' else parameters[key]
    return val

def get_catalog(key='all'):
    conf_paths = ["conf/base", "conf/local"]
    conf_loader = ConfigLoader(conf_paths)
    catalog = conf_loader.get("catalog*", "catalog*/**")
    val = catalog if key == 'all' else catalog[key]
    return val


def get_pandas():
    catalog, parameters = get_config()
    if parameters['engine'] == 'pandas':
        return pandas
    else:
        return modin.pandas