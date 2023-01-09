import re

from config import URL, NAME_APP_RDD, PATH_TO_FILE, PATH_TO_FILE_RESULT
from pyspark.context import SparkContext, RDD


# Просто обёртки над Spark RDD API:
# ----------------------------------------------------------------------------------------------------------------------
def get_file_from_spark(obj: SparkContext, path: str) -> RDD[str]:
    return obj.textFile(path)


def filter_file(obj: RDD, pred) -> RDD[tuple]:
    return obj.filter(pred)


def map_file(obj: RDD, pred) -> RDD:
    return obj.map(pred)


def reduceByKey_obj(obj: RDD, pred) -> RDD[tuple[str, object]]:
    return obj.reduceByKey(pred)


def save_as_text_file_obj(obj: RDD, path: str):
    obj.saveAsTextFile(path)
# ----------------------------------------------------------------------------------------------------------------------


def main():
    # Выполняем подключение к Spark по URL и задаем имя приложения NAME_APP
    sc: SparkContext = SparkContext(URL, NAME_APP_RDD)

    # Получаем объект файла из спарка.
    lines: RDD = get_file_from_spark(sc, PATH_TO_FILE)

    # Отрезаем заголовок в CSV файле, так как он не нужен
    header = lines.first()
    lines_filtered: RDD = filter_file(lines, lambda row: row != header)

    # Делим строки на поля, чтобы применять операции реляционной алгебры
    records: RDD = map_file(lines_filtered, lambda x: x.split(','))

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
    filtered: RDD = filter_file(records, lambda rec: rp_tweet_time.match(rec[tweet_time_index]) and
                                                     rp_is_retweet.match(rec[is_retweet_index]))

    # Получаем отфильтрованный результат.
    result_tuples: RDD = map_file(filtered, lambda rec: (rec[tweet_text_index],
                                                         rec[tweet_time_index],
                                                         rec[is_retweet_index]))

    # Сохраняем результат в файл PATH_TO_FILE_RESULT в ФС
    result_tuples.saveAsTextFile(PATH_TO_FILE_RESULT)


if __name__ == '__main__':
    main()
