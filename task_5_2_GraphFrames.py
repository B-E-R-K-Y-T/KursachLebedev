from graphframes import *

from config import URL, NAME_APP, PATH_TO_FILE, NAME_DB
from pyspark.sql import SparkSession


def main():
    # Подключаемся к ФС спарк. И создаем ссесию БД
    spark = SparkSession.builder \
        .master(URL) \
        .appName(NAME_APP) \
        .getOrCreate()

    # Получаем файл csv из ФС, с которым будем работать.
    data_frame = spark.read.format('csv').options(header='true', inferSchema='true').load(PATH_TO_FILE)

    # Фильтрация данных согласно условию задачи:
    '''
        Найти все твиты, которые ретвитнули, а сам твит 2015 года
    '''

    # Для применения средств Spark SQL необходимо создать представление над data frame
    data_frame.createOrReplaceTempView(NAME_DB)

    # Создание вершин и ребер
    v = spark.sql(f'SELECT tweetid AS id FROM {NAME_DB}')
    e = spark.sql(f'SELECT tweetid AS src, retweet_userid AS dst FROM {NAME_DB} WHERE retweet_userid IS NOT null')
    # Создание графа
    g = GraphFrame(v, e)

    # Отобразить полустепень захода для каждой вершины
    g.inDegrees.show()

    # Найти все твиты, которые ретвитнули
    r = g.find('(a)-[e]->(b)')
    # Показать результат
    r.show()


if __name__ == '__main__':
    main()
