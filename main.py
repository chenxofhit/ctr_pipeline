import time
import os
from config_parser import Config
from logger import LightLogging

LOG_PATH = './logs'

if __name__ == '__main__':

    local_time = time.strftime('%H:%M', time.localtime(time.time()))
    config = Config(config_path='./task_config.yaml')

    pipeline_params = config.pipeline_params
    if local_time == pipeline_params['start_time']:
        logger = LightLogging(log_path=LOG_PATH, log_name='test')
        logger.info('Start Task')
