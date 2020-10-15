import asyncio
from datetime import datetime, timedelta

import pytest

from tests.integration.clickhouse_store import ClickHouseStore


@pytest.mark.asyncio
async def test_async_loading_all_sessions_and_user_session(ch_db: ClickHouseStore):
    """
    This test serves to reproduce the error:
     "clickhouse_driver.errors.UnexpectedPacketFromServerError: Code: 102. Unexpected packet from server clickhouse:9000 (expected Pong, got ProfileInfo)
      /usr/local/lib/python3.6/site-packages/clickhouse_driver/connection.py:391: UnexpectedPacketFromServerError".
    """
    result = await asyncio.gather(
        _get_all_sessions(ch_db),

        _get_user_session(ch_db),
        _get_user_session(ch_db)
    )

    assert result == ['1', '1', '1']


async def _get_all_sessions(ch_db: ClickHouseStore, count: int = 16) -> str:
    for i in range(count):
        now = datetime(2020, 4, 20)

        for _ in ch_db.get_all_sessions((now + timedelta(days=-i), now)):
            await asyncio.sleep(0)

    return '1'


async def _get_user_session(ch_db: ClickHouseStore, count: int = 64) -> str:
    for i in range(count):
        await asyncio.sleep(0)
        ch_db.get_sessions(datetime(2020, 4, 20), 333 + i)

    return '1'
