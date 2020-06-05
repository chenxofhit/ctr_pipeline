import yaml
import argparse


def parse_yaml(config_path):
    with open(config_path) as f:
        config = yaml.load(f, Loader=yaml.FullLoader)

    return config


class Config(object):
    def __init__(self, config_path='./task_config.yaml'):
        config = parse_yaml(config_path)

        self.model_params = config['model']
        self.pipeline_params = config['pipeline']


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument()
    parser.add_argument()

    args = parser.parse_args()
    return args


