from clickhouse_driver import Client
from datetime import date


class ClickHouseStore:
    def __init__(self, client: Client):
        self.client = client

    @staticmethod
    def _build_query(dt: date, user_hash: int) -> str:
        return f'''
            select 
                session, 
                anyLastMerge(session_referer_id) session_referer_id, 
                minMerge(start_time) start_time,
                maxMerge(end_time) end_time
            from pr_session_stats
            where date = '{dt.strftime('%Y-%m-%d')}' and user_hash = {user_hash}
            group by session
        '''

    def get_sessions(self, event_timestamp: date, user_hash: int) -> list:
        stats.incr('ch_get_sessions')
        query = self._build_query(event_timestamp, user_hash)
        rows = self.client.execute(query)
        return [{'session': r[0], 'session_referer_id': r[1], 'start_time': r[2], 'end_time': r[3]} for r in rows]

    def get_all_sessions(self, date_range: tuple) -> list:
        stats.incr('ch_get_all_sessions')
        query = f'''
            select
                user_id,
                session, 
                anyLastMerge(session_referer_id) session_referer_id, 
                minMerge(start_time) start_time,
                maxMerge(end_time) end_time
            from pr_session_stats
            where date >= '{date_range[0].strftime('%Y-%m-%d')}' and date <= '{date_range[1].strftime('%Y-%m-%d')}'
            group by user_id, session
        '''
        # Do not use 'execute_iter'-method instead of 'execute' to avoid error 'UnexpectedPacketFromServerError' (see related test in /tests).
        rows = self.client.execute(query)
        return [{'user_id': r[0], 'session': r[1], 'session_referer_id': r[2], 'start_time': r[3], 'end_time': r[4]} for r in rows]
