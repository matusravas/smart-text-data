import asyncio
import logging.config
import os
import ssl as ssl_module
from datetime import datetime as dt
from ssl import SSLContext
from typing import Union

import yaml
from dotenv import load_dotenv

from app.model import BULK_ACTION

# Loading environment
load_dotenv('config/.env')
env = os.getenv('ENV')
os.environ['ENV'] = env

# log config
with open('config/config.log.yaml', 'r') as f:
    log_config = yaml.safe_load(f.read())
    log_config['handlers']['info_file_handler']['filename'] = 'logs/info_{}.log'.format(dt.now().strftime('%Y-%m-%d'))
    log_config['handlers']['error_file_handler']['filename'] = 'logs/errors_{}.log'.format(dt.now().strftime('%Y-%m-%d'))
    logging.config.dictConfig(log_config)
logger = logging.getLogger(__name__)

app_config_env_path = 'config/config.dev.yaml' if env == 'development' else 'config/config.test.yaml' if env == 'test' else 'config/config.prod.yaml'


# Loading configurations
app_config_path = 'config/config.app.yaml'
app_config_env_path = 'config/config.dev.yaml' if env == 'development' else 'config/config.test.yaml' if env == 'test' else 'config/config.prod.yaml'
try:
    with open(app_config_env_path, 'r') as f:
        app_config_env = yaml.safe_load(f.read())
    with open(app_config_path, 'r', encoding='utf-8') as f:
        app_config = yaml.safe_load(f.read())
except OSError as e:
    raise Exception(f'Unable to open {e.filename}')

# ssl config
cert_path = app_config_env.get('ssl', {}).get('cert_path', None)
key_path = app_config_env.get('ssl', {}).get('key_path', None)

ssl: Union[bool, SSLContext] = False
if env in ('test', 'production'):
    sslcontext = ssl_module.create_default_context()
    sslcontext.load_cert_chain(cert_path, key_path)
    ssl = sslcontext

# elasticsearch config
base_es_url = app_config_env['elasticsearch']['url']
es_hosts = app_config_env['elasticsearch']['hosts']
es_auth_token = app_config_env['elasticsearch']['auth_token'] if 'auth_token' in app_config_env['elasticsearch'] else ''
es_headers = app_config['elasticsearch']['headers']
es_headers['Authorization'] = es_auth_token


WORK_DIR = app_config['export']['absolute_path']
FILE_EXTENSIONS = tuple(app_config['export']['file_extensions'])
TIMESTAMP = dt.now().timestamp()
try:
    ACTION: BULK_ACTION = [ba for ba in BULK_ACTION if ba.value == app_config['export']['bulk_action']][0]
except:
    ACTION: BULK_ACTION = BULK_ACTION.INDEX


asyncio.set_event_loop(asyncio.new_event_loop())