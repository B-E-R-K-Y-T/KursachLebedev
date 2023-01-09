from config import URL, NAME_APP, PATH_TO_FILE, NAME_DB
from pyspark.sql import SparkSession


# Просто обёртки над Spark SQL API:
# ----------------------------------------------------------------------------------------------------------------------
def get_file_csv_from_spark(obj: SparkSession, path: str, infer_schema, header, sep):
    return obj.read.csv(path, inferSchema=infer_schema, header=header, sep=sep)


# ----------------------------------------------------------------------------------------------------------------------


def main():
    # Подключаемся к ФС спарк. И создаем ссесию БД
    spark = SparkSession.builder \
        .master(URL) \
        .appName(NAME_APP) \
        .getOrCreate()

    # Получаем файл csv из ФС, с которым будем работать.
    data_frame = get_file_csv_from_spark(spark, PATH_TO_FILE, infer_schema=True, header=True, sep=',')
    # Просмотреть, то, что было загружено:
    print('Data set: ')
    data_frame.show()

    # Для применения средств Spark SQL необходимо создать представление над data frame
    data_frame.createGlobalTempView(NAME_DB)

    # Фильтрация данных согласно условию задачи:
    '''
        Найти все твиты, которые ретвитнули, а сам твит 2015 года
    '''
    tweet_text = 'tweet_text'
    tweet_time = 'tweet_time'
    is_retweet = 'is_retweet'

    # SQL запрос к представлению в виде БД нашего файла, с целью получения нужных нам данных
    request_sql = f"SELECT {tweet_text}, {tweet_time}, {is_retweet} " \
                  f"FROM global_temp.{NAME_DB} " \
                  f"WHERE ({is_retweet} = True) AND ({tweet_time} LIKE '2015%')"
    # Вывод данных на экран.
    spark.sql(request_sql).show()


if __name__ == '__main__':
    main()
