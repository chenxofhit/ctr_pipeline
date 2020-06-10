import time
import os
from config_parser import Config
from logger import LightLogging
import sentry_sdk
from odps_process import

LOG_PATH = './logs'

if __name__ == '__main__':

    local_time = time.strftime('%H:%M', time.localtime(time.time()))
    config = Config(config_path='./task_config.yaml')

    pipeline_params = config.pipeline_params
    while True:
        if local_time in pipeline_params['start_time']:
            print('Start task')




