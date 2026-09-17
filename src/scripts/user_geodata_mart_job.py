import sys

from pyspark.sql import SparkSession  # type: ignore
import pyspark.sql.functions as F  # type: ignore
from pyspark.sql.window import Window  # type: ignore

from utils import city_geo_read, get_messages, get_distance_km, full_events_read


def define_message_city(messages, city_geo):
    """
    Определяет ближайший город для каждого сообщения.
    """
    messages = messages.withColumnRenamed('lat', 'lat_1').withColumnRenamed(
        'lon', 'lon_1'
    )

    city_geo = city_geo.withColumnRenamed('lat', 'lat_2').withColumnRenamed(
        'lng', 'lon_2'
    )

    messages_cities_candidates = messages.crossJoin(F.broadcast(city_geo))

    # Окно для поиска ближайшего города.
    # Если расстояние до нескольких городов одинаковое, берем первый по алфавиту.
    dist_window = Window.partitionBy('message_id').orderBy('distance_to_city', 'city')

    messages_geo = (
        messages_cities_candidates.withColumn(
            'distance_to_city',
            get_distance_km(
                F.col('lat_1'), F.col('lon_1'), F.col('lat_2'), F.col('lon_2')
            ),
        )
        .withColumn('rn', F.row_number().over(dist_window))
        .filter(F.col('rn') == 1)  # оставляем ближайший город для каждого сообщения
        .select('message_id', 'user_id', 'city', 'datetime', 'timezone')
    )

    return messages_geo


def user_geo_mart_process(messages_geo):
    """
    Основная функция для рассчета витрины геоданных в разрезе пользователя.
    """
    # Окно для поиска последнего сообщения от каждого пользователя.
    # Если есть несколько сообщений с одинаковым временем,
    # то берем то у которого больше message_id.
    act_window = Window.partitionBy('user_id').orderBy(
        F.col('datetime').desc(), F.col('message_id').desc()
    )

    user_act_cities = (
        messages_geo.withColumn('rn', F.row_number().over(act_window))
        .filter(F.col('rn') == 1)  # только последнее сообщение для каждого пользователя
        .select(
            'user_id',
            F.col('city').alias('act_city'),
            F.col('datetime').alias('TIME_UTC'),
            'timezone',
        )
    )

    # Получаем список всех городов из которых отправлялось хотя бы одно сообщение в день
    # для каждого пользователя.
    user_daily_geo = messages_geo.select(
        'user_id', 'city', F.to_date('datetime').alias('date')
    ).distinct()

    # Окно для последовательности пребывания пользователя в городе по дням.
    city_sequence_window = Window.partitionBy('user_id', 'city').orderBy('date')

    geo_stay_periods = user_daily_geo.withColumn(
        'date_group',
        F.date_sub(F.col('date'), F.row_number().over(city_sequence_window)),
    )

    # Окно для поиска последнего города в котором пользователь прибывал
    # 27 дней и более. Если городов несколько, берем по алфавиту.
    latest_city_window = Window.partitionBy('user_id').orderBy(
        F.col('end_date').desc(), 'city'
    )

    # Группируем пользователя, город и последовательность с
    # агрегацией по колличеству непрерывных дней.
    user_home_cities = (
        geo_stay_periods.groupBy('user_id', 'city', 'date_group')
        .agg(F.max('date').alias('end_date'), F.count('date').alias('days_count'))
        .filter(F.col('days_count') >= 27)  # только те, в которых был от 27 дней
        .withColumn('rn', F.row_number().over(latest_city_window))
        .filter(F.col('rn') == 1)  # оставляем последний подходящий город.
        .select('user_id', F.col('city').alias('home_city'))
    )

    # Соединеям для каждого пользоавателя его последний город и домашний.
    # Обязательно left_join так как далеко не у всех пользователей есть город,
    # из которого 27 дней подряд отправлялись сообщения.
    user_geo_mart_base = user_act_cities.join(user_home_cities, 'user_id', 'left')

    # Окно для определения истории передвижения пользователя по городам.
    travel_window = Window.partitionBy('user_id').orderBy('datetime')

    user_movements = (
        messages_geo.withColumn('prev_city', F.lag('city').over(travel_window))
        # Оставляем первый город и следующие последующие измененные.
        .where('prev_city is null or city != prev_city').select('user_id', 'city')
    )

    # Для каждого пользователя получаем число смен городов
    # и список измененных городов в хронологическом порядке.
    user_travels = user_movements.groupBy('user_id').agg(
        F.count('city').alias('travel_count'),
        F.collect_list('city').alias('travel_array'),
    )

    # Собираем итоговую витрину и вычисляем оставшиеся поля.
    # На всякий случай left_join, хотя по логике тут можно и inner.
    user_geo_mart = (
        user_geo_mart_base.join(user_travels, 'user_id', 'left')
        .withColumn(
            'local_time', F.from_utc_timestamp(F.col('TIME_UTC'), F.col('timezone'))
        )
        .drop('TIME_UTC', 'timezone')
    )

    return user_geo_mart


def main():

    APP_NAME = 'UserGeodataJob'

    SOURCE_DIR = sys.argv[1]
    DEST_DIR = sys.argv[2]
    CITY_GEO = sys.argv[3]

    spark = SparkSession.builder.master('yarn').appName(APP_NAME).getOrCreate()

    # Читаем данные о городах из .csv справочника.
    city_geo = city_geo_read(spark, CITY_GEO)

    # Читаем все данные за всё время из фактов events.
    full_events = full_events_read(spark, SOURCE_DIR)

    # Достаем только сообщения и чистим дубликаты.
    messages = get_messages(full_events)

    # Определяем ближайший город для каждого отправленного сообщения.
    messages_geo = define_message_city(messages, city_geo)

    # Вычисляем витрину.
    user_geo_mart = user_geo_mart_process(messages_geo)

    # Пишем результат.
    user_geo_mart.write.mode('overwrite').parquet(DEST_DIR)

    spark.stop()


if __name__ == "__main__":
    main()