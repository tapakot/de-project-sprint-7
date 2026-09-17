import sys

from pyspark.sql import SparkSession  # type: ignore
import pyspark.sql.functions as F  # type: ignore
from pyspark.sql.window import Window  # type: ignore

from utils import (
    get_distance_km,
    full_events_read,
    get_messages,
    user_geo_read,
    city_geo_read,
    get_user_zone_ids,
)


def friend_recommendations_mart_process(full_events, user_zone_ids):
    """
    Основная функция для расчета витрины для рекомендации друзей.
    """
    # Достаем только сообщения и чистим дубликаты.
    messages = get_messages(full_events)

    # Окно для расчета координат последнего отправленного сообщения
    # от каждого пользователя.
    act_window = Window.partitionBy('user_id').orderBy(
        F.col('datetime').desc(), F.col('message_id').desc()
    )

    user_act_geo = (
        messages.withColumn('rn', F.row_number().over(act_window))
        .filter(F.col('rn') == 1)  # оставляем последнее сообщение
        .select('user_id', 'lat', 'lon', 'datetime')
    )

    # Выбираем все подписки пользователей на каналы.
    user_channels = (
        full_events.where(
            'event_type = "subscription" and event.subscription_channel is not null'
        )
        # Упрощение для учебного проекта иначе джоба падает по памяти!!!
        .filter(F.col('date') == '2022-05-06').select(
            F.col('event.user').alias('user_id'),
            F.col('event.subscription_channel').alias('channel_id'),
        )
        # Убираем дубли на случай если пользователь подписался
        # на один и тот-же канал несколько раз.
        .distinct()
    )

    # Формируем пары кандидатов в друзья на основе подписок на общие каналы.
    # Используем Self-Join (самоприсоединение) таблицы подписок.
    friend_candidates = (
        user_channels.alias('u1')
        .join(
            user_channels.alias('u2'),
            (F.col('u1.channel_id') == F.col('u2.channel_id'))
            # Условие '<' исключает дубликаты пар (A-B и B-A)
            # и предотвращает сравнение пользователя самого с собой (A-A).
            & (F.col('u1.user_id') < F.col('u2.user_id')),
            'inner',
        )
        .select(
            F.col('u1.user_id').alias('user_left'),
            F.col('u2.user_id').alias('user_right'),
        )
        # Убираем дубли, если пользователи пересекаются в нескольких каналах сразу.
        .distinct()
    )

    # Берем всех отправителей и получателей сообщений.
    raw_connections = messages.where('receiver_id is not null').select(
        'user_id', 'receiver_id'
    )

    # Получаем пары всех кто переписвался.
    known_contacts = (
        raw_connections.select(
            F.col('user_id').alias('user_left'),
            F.col('receiver_id').alias('user_right'),
        )
        .union(
            raw_connections.select(
                F.col('receiver_id').alias('user_left'),
                F.col('user_id').alias('user_right'),
            )
        )
        .distinct()
    )

    # Убираем из пар кандидатов в друзья тех кто уже переписывались.
    not_contact_user_pairs = friend_candidates.join(
        known_contacts, ['user_left', 'user_right'], 'left_anti'
    )

    # Добавляем оставшимся парам кандидатов их координаты.
    friend_candidates_geo = not_contact_user_pairs.join(
        user_act_geo.select(
            F.col('user_id').alias('user_left'),
            F.col('lat').alias('lat_l'),
            F.col('lon').alias('lon_l'),
            'datetime',
        ),
        'user_left',
        'inner',
    ).join(
        user_act_geo.select(
            F.col('user_id').alias('user_right'),
            F.col('lat').alias('lat_r'),
            F.col('lon').alias('lon_r'),
        ),
        'user_right',
        'inner',
    )

    # Вычисляем дистанцию между кандидатами и оставляем только тех
    # кто находится в радиусе не более 1 км друг от друга.
    final_recommendations = (
        friend_candidates_geo.withColumn(
            'distance',
            get_distance_km(
                F.col('lat_l'), F.col('lon_l'), F.col('lat_r'), F.col('lon_r')
            ),
        )
        .filter(F.col('distance') <= 1.0)
        .select('user_left', 'user_right', 'datetime')
    )

    # Собираем итоговую витрину и вычисляем оставшиеся поля.
    # Информацию о геозоне и локальном времени берем для user_left.
    friends_recommendations_mart = (
        final_recommendations.join(
            user_zone_ids.withColumnRenamed('user_id', 'user_left'), 'user_left', 'left'
        )
        .withColumn('processed_dttm', F.current_timestamp())
        .withColumn(
            'local_time',
            F.from_utc_timestamp(F.col('datetime'), F.col('timezone')),
        )
        .select('user_left', 'user_right', 'processed_dttm', 'zone_id', 'local_time')
    )

    return friends_recommendations_mart


def main():

    APP_NAME = 'FriendRecommendationsJob'

    SOURCE_DIR = sys.argv[1]
    DEST_DIR = sys.argv[2]
    CITY_GEO = sys.argv[3]
    USER_GEO = sys.argv[4]

    spark = SparkSession.builder.master('yarn').appName(APP_NAME).getOrCreate()

    # Читаем все данные за всё время из фактов events.
    full_events = full_events_read(spark, SOURCE_DIR)

    # Читаем данные о городах из .csv справочника.
    city_geo = city_geo_read(spark, CITY_GEO)

    # Читаем данные об актуальных геопозициях пользователей из первой витрины.
    user_geo = user_geo_read(spark, USER_GEO)

    # Определяем zone_id для каждого пользователя.
    user_zone_ids = get_user_zone_ids(user_geo, city_geo)

    # Вычисляем витрину.
    friends_recommendations_mart = friend_recommendations_mart_process(
        full_events, user_zone_ids
    )

    # Пишем результат.
    friends_recommendations_mart.write.mode('overwrite').parquet(DEST_DIR)

    spark.stop()


if __name__ == "__main__":
    main()