from pyspark.sql import SparkSession

# url нашей ФС
URL = 'local'
# Название приложения(Не понял зачем, но указать его надо!)
NAME_APP = 'app_task_5_1_SQL'


# Просто обёртки над Spark SQL API:
# ----------------------------------------------------------------------------------------------------------------------
def get_file_csv_from_spark(obj: SparkSession, path: str, infer_schema, header, sep):
    return obj.read.csv(path, inferSchema=infer_schema, header=header, sep=sep)


# ----------------------------------------------------------------------------------------------------------------------


def main():
    spark = SparkSession.builder \
        .master(URL) \
        .appName(NAME_APP) \
        .getOrCreate()

    # Тут должен быть путь до файла с дампом твиттера, который надо закинуть в Spark (hdfs:///user/...)
    path_to_file = 'data/tweets.csv'
    data_frame = get_file_csv_from_spark(spark, path_to_file, infer_schema=True, header=True, sep=',')
    # Просмотреть, то, что было загружено:
    print('Data set: ')
    data_frame.show()

    # Для применения средств Spark SQL необходимо создать представление над data frame
    data_frame.createGlobalTempView('tweets')

    # Фильтрация данных согласно условию задачи:
    '''
        Найти все твиты, которые ретвитнули, а сам твит 2015 года
    '''

    request_sql = "SELECT tweet_text, tweet_time, is_retweet FROM global_temp.tweets WHERE (is_retweet = True) AND (tweet_time LIKE '2015%')"
    spark.sql(request_sql).show()


if __name__ == '__main__':
    main()
