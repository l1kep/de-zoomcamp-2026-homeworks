"""
Demo aggregation job with 10-second tumbling windows.

Use with producer_realtime.py to observe watermark behavior:
- Watermark = event_timestamp - 5 seconds
- Late events (<=5s) arrive before the watermark closes the window -> included
- Late events (>5s) may arrive after the watermark closes the window -> dropped
"""

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

def create_events_source_kafka(t_env):
    table_name = "events"
    source_ddl = f"""
        CREATE TABLE {table_name} (
            lpep_pickup_datetime VARCHAR,
            lpep_dropoff_datetime VARCHAR,
            pulocationid INTEGER,
            dolocationid INTEGER,
            passenger_count INTEGER,
            trip_distance DOUBLE,
            tip_amount DOUBLE,
            total_amount DOUBLE,
            event_timestamp AS TO_TIMESTAMP(lpep_pickup_datetime, 'yyyy-MM-dd HH:mm:ss'),
            WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '10' MINUTE
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = 'redpanda:29092',
            'topic' = 'green-trips',
            'scan.startup.mode' = 'earliest-offset',
            'properties.auto.offset.reset' = 'latest',
            'format' = 'json'
        );
        """
    t_env.execute_sql(source_ddl)
    return table_name


def create_events_aggregated_sink(t_env):
    table_name = 'processed_events_aggregated_3'
    sink_ddl = f'''
        CREATE TABLE {table_name} (
            window_start TIMESTAMP(3),
            total_tip_amount DECIMAL,
            PRIMARY KEY (window_start) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = '{table_name}',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        );
        
        '''
    print(sink_ddl)
    t_env.execute_sql(sink_ddl)
    return table_name


def log_aggregation():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)
    env.set_parallelism(1)

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    try:
        source_table = create_events_source_kafka(t_env)
        aggregated_table = create_events_aggregated_sink(t_env)

        # 10-second tumbling windows (instead of 1 hour) so we can
        # observe windows closing and late events being dropped
        t_env.execute_sql(f'''
            INSERT INTO {aggregated_table}
            SELECT
                window_start,  -- 👈 Уже обрезан до начала часа (18:00:00)
                SUM(tip_amount) AS total_tip_amount
            FROM TABLE(
                TUMBLE(TABLE {source_table}, DESCRIPTOR(event_timestamp), INTERVAL '1' HOUR)
            )
            GROUP BY window_start;
        ''').wait()
    except Exception as e:
        print("Writing records from Kafka to JDBC failed:", str(e))


if __name__ == '__main__':
    log_aggregation()
