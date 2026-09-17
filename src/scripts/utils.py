import pyspark.sql.functions as F  # type: ignore
from pyspark.sql.window import Window  # type: ignore


def full_events_read(spark, source_path):
    """
    Вычитывает все события из таблицы фактов events.
    """
    full_events = spark.read.option('basePath', source_path).parquet(source_path)

    return full_events


def city_geo_read(spark, source_path):
    """
    Вычитывает справочную геоинформацию о городах Австралии из .csv
    """
    city_geo = (
        spark.read.option('header', 'true')
        .option('sep', ';')
        .option('inferSchema', 'true')
        .csv(source_path)
    )

    return city_geo


def get_messages(events):
    """
    Получает информацию о сообщениях из общих фактов.
    """
    # Окно для удаления сообщений с одинаковым message_id,
    # но с разными остальными данными. Оставляем только самое последнее сообщение
    # или сообщение с более высоким user_id в случае совпадения времени.
    dedup_window = Window.partitionBy('message_id').orderBy(
        F.col('datetime').desc(), F.col('user_id').desc()
    )

    messages = (
        events.filter(F.col('event_type') == 'message')
        .select(
            'event.message_id',
            F.col('event.message_from').alias('user_id'),
            F.col('event.message_to').alias('receiver_id'),
            'lat',
            'lon',
            # Заметил что в данных есть две колонки с датой и временем отправки
            # сообщения и при этом в колонке message_ts время "съехало" на 1 год.
            F.coalesce(
                'event.datetime',
                F.col('event.message_ts').cast('timestamp') + F.expr('INTERVAL 1 YEAR'),
            ).alias('datetime'),
        )
        .withColumn('rn', F.row_number().over(dedup_window))
        .where('rn = 1')  # оставляем только уникальные message_id
        .drop('rn')
    )

    return messages


def get_distance_km(lat_1, lon_1, lat_2, lon_2):
    """
    Вычисляет расстояние между двумя точками на сфере (в км).
    """
    R = 6371.0  # Радиус Земли в км

    lat_1 = F.radians(lat_1)
    lon_1 = F.radians(lon_1)
    lat_2 = F.radians(lat_2)
    lon_2 = F.radians(lon_2)

    dlat = (lat_2 - lat_1) / 2
    dlon = (lon_2 - lon_1) / 2
    a = F.sin(dlat) ** 2 + F.cos(lat_1) * F.cos(lat_2) * F.sin(dlon) ** 2

    return 2 * R * F.asin(F.sqrt(a))


def user_geo_read(spark, source_path):
    """
    Вычитывает витрину с геоданными пользователей.
    """
    user_geo = spark.read.parquet(source_path)

    return user_geo


def get_user_zone_ids(user_geo, city_geo):
    """
    Получает информацию об актуальных zone_id и timezone для каждого пользователя.
    """
    user_zone_ids = (
        user_geo.select('user_id', F.col('act_city').alias('city'))
        .join(F.broadcast(city_geo), 'city')
        .select('user_id', F.col('id').alias('zone_id'), 'timezone')
    )

    return user_zone_ids