import clickhouse_connect


class XAPILake:
    def __init__(self, host, port, username, password=None, database=None):
        self.host = host
        self.port = port
        self.username = username
        self.database = database

        self.event_table_name = 'xapi_events_all'
        self.event_buffer_table_name = 'xapi_events_buffered_all'

        self.client = clickhouse_connect.get_client(
            host=self.host,
            username=self.username,
            password=password,
            port=self.port,
            database=self.database,
            date_time_input_format="best_effort",  # Allows RFC dates
            old_parts_lifetime=10  # Seconds, reduces disk usage
        )

    def print_db_time(self):
        res = self.client.query("SELECT timezone(), now()")
        print(res.result_set)

    def print_row_counts(self):
        res = self.client.query(f'SELECT count(*) FROM {self.event_buffer_table_name}')
        print("Buffer table row count:")
        print(res.result_set)

        print("Hard table row count:")
        res = self.client.query(f'SELECT count(*) FROM {self.event_table_name}')
        print(res.result_set)

    def create_db(self):
        self.client.command(f"CREATE DATABASE IF NOT EXISTS {self.database}")

    def drop_tables(self):
        self.client.command(f"DROP TABLE IF EXISTS {self.event_buffer_table_name}")
        self.client.command(f"DROP TABLE IF EXISTS {self.event_table_name}")
        print("Tables dropped")

    def create_tables(self):
        self.client.command(f"""
            CREATE TABLE IF NOT EXISTS {self.event_table_name} (
            event_id UUID NOT NULL,
            verb String NOT NULL,
            actor_id UUID NOT NULL,
            org String NOT NULL,
            course_run_id String NOT NULL,
            problem_id String NULL,
            video_id String NULL,
            nav_starting_point String NULL,
            nav_ending_point String NULL,
            emission_time timestamp NOT NULL,
            event String NOT NULL
            )
            ENGINE MergeTree ORDER BY (course_run_id, verb, emission_time)
            PRIMARY KEY (course_run_id, verb)
        """)

        self.client.command(f"""
            CREATE TABLE IF NOT EXISTS {self.event_buffer_table_name} AS {self.event_table_name} 
            ENGINE = Buffer(
                currentDatabase(), 
                {self.event_table_name}, 
                16, 
                10, 
                100, 
                10000, 
                1000000, 
                10000000, 
                100000000
            )
        """)

        print("Tables created")

    def batch_insert(self, events):
        """
            event_id UUID NOT NULL,
            verb String NOT NULL,
            actor_id UUID NOT NULL,
            org UUID NOT NULL,
            course_run_id String NULL,
            problem_id String NULL,
            video_id String NULL,
            nav_starting_point String NULL,
            nav_ending_point String NULL,
            emission_time timestamp NOT NULL,
            event String NOT NULL
        """
        out_data = []
        for v in events:
            try:
                out = f"('{v['event_id']}', '{v['verb']}', '{v['actor_id']}', '{v['org']}', "

                out += f"'{v['course_run_id']}', " if 'course_run_id' in v else "NULL, "
                out += f"'{v['problem_id']}', " if 'problem_id' in v else "NULL, "
                out += f"'{v['video_id']}', " if 'video_id' in v else "NULL, "
                out += f"'{v['nav_starting_point']}', " if 'nav_starting_point' in v else "NULL, "
                out += f"'{v['nav_ending_point']}', " if 'nav_ending_point' in v else "NULL, "

                out += f"'{v['emission_time']}', '{v['event']}')"
                out_data.append(out)
            except:
                print(v)
                raise
        vals = ",".join(out_data)

        # from pprint import pprint
        # pprint(out_data)

        self.client.command(f"""
            INSERT INTO {self.event_buffer_table_name} (
                event_id, 
                verb, 
                actor_id, 
                org,
                course_run_id, 
                problem_id, 
                video_id, 
                nav_starting_point, 
                nav_ending_point, 
                emission_time, 
                event
            )
            VALUES {vals}
        """)
