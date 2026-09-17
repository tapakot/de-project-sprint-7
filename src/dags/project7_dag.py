from airflow.decorators import dag  # type: ignore
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator  # type: ignore
import pendulum  # type: ignore


@dag(
    schedule='0 3 * * *',
    start_date=pendulum.datetime(2026, 3, 15),
    catchup=True,
    tags=['spark', 'geo'],
)
def spark_job():

    env_vars = {
        'HADOOP_CONF_DIR': '/etc/hadoop/conf/',
        'YARN_CONF_DIR': '/etc/hadoop/conf/',
        'JAVA_HOME': '/usr',
        'SPARK_HOME': '/usr/lib/spark',
        'PYSPARK_PYTHON': '/usr/bin/python3',
    }

    spark_resources = {
        'num_executors': 5,
        'executor_cores': 2,
        'executor_memory': '4g',
        'driver_memory': '4g',
        'conf': {
            'spark.yarn.am.cores': '1',
            'spark.dynamicAllocation.enabled': 'false',
        },
    }

    raw_to_ods_job = SparkSubmitOperator(
        task_id='raw_to_ods_job',
        application='/lessons/raw_to_ods_job.py',
        py_files='/lessons/utils.py',
        conn_id='yarn_spark',
        **spark_resources,
        env_vars=env_vars,
        application_args=[
            '{{ ds }}',
            '/user/master/data/geo/events',
            '/user/null2de/data/geo/events',
        ],
        retries=2,
        retry_delay=pendulum.duration(minutes=5),
    )

    user_geodata_mart_job = SparkSubmitOperator(
        task_id='user_geodata_mart_job',
        application='/lessons/user_geodata_mart_job.py',
        py_files='/lessons/utils.py',
        conn_id='yarn_spark',
        **spark_resources,
        env_vars=env_vars,
        application_args=[
            '/user/null2de/data/geo/events',
            '/user/null2de/data/analytics/users_geo',
            '/user/null2de/data/geo_2.csv',
        ],
        retries=2,
        retry_delay=pendulum.duration(minutes=5),
    )

    geo_activity_stats_mart_job = SparkSubmitOperator(
        task_id='geo_activity_stats_mart_job',
        application='/lessons/geo_activity_stats_mart_job.py',
        py_files='/lessons/utils.py',
        conn_id='yarn_spark',
        **spark_resources,
        env_vars=env_vars,
        application_args=[
            '/user/null2de/data/geo/events',
            '/user/null2de/data/analytics/geo_activity_stats',
            '/user/null2de/data/geo_2.csv',
            '/user/null2de/data/analytics/users_geo',
        ],
        retries=2,
        retry_delay=pendulum.duration(minutes=5),
    )

    friend_recommendations_mart_job = SparkSubmitOperator(
        task_id='friend_recommendations_mart_job',
        application='/lessons/friend_recommendations_mart_job.py',
        py_files='/lessons/utils.py',
        conn_id='yarn_spark',
        **spark_resources,
        env_vars=env_vars,
        application_args=[
            '/user/null2de/data/geo/events',
            '/user/null2de/data/analytics/friend_recommendations',
            '/user/null2de/data/geo_2.csv',
            '/user/null2de/data/analytics/users_geo',
        ],
        retries=2,
        retry_delay=pendulum.duration(minutes=5),
    )

    (
        raw_to_ods_job
        >> user_geodata_mart_job
        >> [geo_activity_stats_mart_job, friend_recommendations_mart_job]
    )


spark_dag = spark_job()