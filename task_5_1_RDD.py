# запустить можно с помощью команды, поменяв {username} на имя своего пользователя(Я ещё добавил 3-й аргумент: path):
# python3 consultant_for_pairs.py localhost 50070 {username} {path}
import re
import contextlib
import sys
import pyhdfs
import ast

from config import URL, NAME_APP_RDD, PATH_TO_FILE, PATH_TO_FILE_RESULT
from pyspark.context import SparkContext
from collections import Counter


# Просто обёртки над Spark RDD API:
# ----------------------------------------------------------------------------------------------------------------------
def get_file_from_spark(obj, path):
    return obj.textFile(path)


def filter_file(obj, pred):
    return obj.filter(pred)


def map_file(obj, pred):
    return obj.map(pred)


def reduceByKey_obj(obj, pred):
    return obj.reduceByKey(pred)


def save_as_text_file_obj(obj, path):
    obj.saveAsTextFile(path)


# ----------------------------------------------------------------------------------------------------------------------


def main():
    server = sys.argv[1]
    port = sys.argv[2]
    username = sys.argv[3]
    # если будут ошибки, то они могут быть из-за этой строки
    path = sys.argv[4] if len(sys.argv) > 3 else f"/user/{username}/laba2_1/output"
    file_name = "/part-00000"

    fs = pyhdfs.HdfsClient(hosts=f'{server}:{port}', user_name=username)

    # Выполняем подключение к Spark по URL и задаем имя приложения NAME_APP
    sc = SparkContext(URL, NAME_APP_RDD)

    # Получаем объект файла из спарка.
    lines = get_file_from_spark(sc, PATH_TO_FILE)

    # Отрезаем заголовок в CSV файле, так как он не нужен
    header = lines.first()
    lines_filtered = filter_file(lines, lambda row: row != header)

    # Делим строки на поля, чтобы применять операции реляционной алгебры
    records = map_file(lines_filtered, lambda x: x.split(','))

    # Фильтрация данных согласно условию задачи:
    '''
        Найти все твиты, которые ретвитнули, а сам твит 2015 года
    '''
    # Такие у них индексы в CSV, но мб, я мог написать на единицу больше или меньше, так как делаю это все вслепую,
    # ибо Саня пока что не поднял спарк
    tweet_text_index = 13
    tweet_time_index = 14
    is_retweet_index = 19

    # Инициализируем два паттерна регулярных выражений для поиска соответствующих вхождений.
    rp_tweet_time = re.compile('2015')
    rp_is_retweet = re.compile('TRUE')
    # Производим фильтрацию данных по паттернам в файловой системе.
    filtered = filter_file(records, lambda rec: rp_tweet_time.match(rec[tweet_time_index]) and
                                                rp_is_retweet.match(rec[is_retweet_index]))

    # Получаем отфильтрованный результат.
    result_tuples = map_file(filtered, lambda rec: (rec[tweet_text_index],
                                                    rec[tweet_time_index],
                                                    rec[is_retweet_index]))

    # Сохраняем результат в файл PATH_TO_FILE_RESULT в ФС
    result_tuples = result_tuples.toDF().toJSON()
    result_tuples.saveAsTextFile(PATH_TO_FILE_RESULT)


if __name__ == '__main__':
    main()
