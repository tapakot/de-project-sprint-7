import sys

from pyspark.sql import SparkSession  # type: ignore
import pyspark.sql.functions as F  # type: ignore
from pyspark.sql.window import Window  # type: ignore

from utils import city_geo_read, full_events_read, user_geo_read, get_user_zone_ids


def geo_activity_stats_mart_process(full_events, user_zone_ids):
    """
    Основная функция для расчета витрины активности по городам.
    """
    user_activity_base = full_events.select(
        F.date_trunc('month', F.col('date')).alias('month'),
        F.date_trunc('week', F.col('date')).alias('week'),
        F.coalesce('event.message_from', 'event.reaction_from', 'event.user').alias(
            'user_id'
        ),
        'event_type',
    )

    # Вычисляем дату регистрации для каждого пользователя
    # как дату отправки первого сообщения.
    user_registrations = (
        full_events.filter(F.col('event_type') == 'message')
        .select(F.col('event.message_from').alias('user_id'), 'date')
        .groupBy('user_id')
        .agg(F.min('date').alias('registration_date'))
    )

    # Создаем список событий регистраций для каждого пользователя.
    registration_events = user_registrations.select(
        F.date_trunc('month', F.col('registration_date')).alias('month'),
        F.date_trunc('week', F.col('registration_date')).alias('week'),
        'user_id',
        F.lit('registration').alias('event_type'),
    )

    # Добавляем регистрации пользователей к остальным событиям.
    full_user_activity = user_activity_base.unionByName(
        registration_events.select(user_activity_base.columns)
    )

    # Добавляем zone_id для каждого события. Используется inner, так как в условии:
    # 'Пока присвойте таким событиям координаты последнего отправленного сообщения
    # конкретного пользователя.'
    full_user_activity_geo = full_user_activity.join(user_zone_ids, 'user_id').drop(
        'user_id', 'timezone'
    )

    # Окно для расчета событий по геозонам в разрезе месяца.
    month_window = Window.partitionBy('month', 'zone_id')

    # Финальная группировка и подсчет событий в разрезе недели и месяца.
    geo_activity_stats_mart = (
        full_user_activity_geo.groupBy('month', 'week', 'zone_id')
        .agg(
            F.count(F.when(F.col('event_type') == 'message', 1)).alias('week_message'),
            F.count(F.when(F.col('event_type') == 'reaction', 1)).alias(
                'week_reaction'
            ),
            F.count(F.when(F.col('event_type') == 'subscription', 1)).alias(
                'week_subscription'
            ),
            F.count(F.when(F.col('event_type') == 'registration', 1)).alias(
                'week_user'
            ),
        )
        .withColumn('month_message', F.sum('week_message').over(month_window))
        .withColumn('month_reaction', F.sum('week_reaction').over(month_window))
        .withColumn('month_subscription', F.sum('week_subscription').over(month_window))
        .withColumn('month_user', F.sum('week_user').over(month_window))
    )

    return geo_activity_stats_mart


def main():

    APP_NAME = 'GeoActivityStatsJob'

    SOURCE_DIR = sys.argv[1]
    DEST_DIR = sys.argv[2]
    CITY_GEO = sys.argv[3]
    USER_GEO = sys.argv[4]

    spark = SparkSession.builder.master('yarn').appName(APP_NAME).getOrCreate()

    # Читаем все данные за всё время из фактов events.
    full_events = full_events_read(spark, SOURCE_DIR)

    # Читаем данные о городах из .csv справочника
    city_geo = city_geo_read(spark, CITY_GEO)

    # Читаем данные об актуальных геопозициях пользователей из первой витрины.
    user_geo = user_geo_read(spark, USER_GEO)

    # Определяем zone_id для каждого пользователя.
    user_zone_ids = get_user_zone_ids(user_geo, city_geo)

    # Вычисляем витрину.
    geo_activity_stats_mart = geo_activity_stats_mart_process(
        full_events, user_zone_ids
    )

    # Пишем результат.
    geo_activity_stats_mart.write.mode('overwrite').parquet(DEST_DIR)

    spark.stop()


if __name__ == "__main__":
    main()