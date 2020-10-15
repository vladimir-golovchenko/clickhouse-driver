import os

import pytest
from clickhouse_driver import Client
from tests.integration import config
from tests.integration.clickhouse_store import ClickHouseStore


def get_path():
    return os.path.dirname(os.path.realpath(__file__))


def read_scripts(filename):
    with open(filename, 'r') as f:
        script = ''
        for line in f.readlines():
            if (line.isspace() or line.lstrip().startswith('--')) and script:
                yield script
                script = ''
            else:
                script += line
        if script:
            yield script


@pytest.fixture(scope='session')
def ch_db() -> ClickHouseStore:
    ch_client = Client(config.CH_HOST,
                       port=config.CH_PORT,
                       database=config.CH_DATABASE,
                       user=config.CH_USER,
                       password=config.CH_PASSWORD)

    for script in read_scripts(os.path.join(get_path(), 'clickhouse')):
        ch_client.execute(script)
    return ClickHouseSessionStore(client=ch_client)
