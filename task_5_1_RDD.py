import re

from pyspark.context import SparkContext, RDD

# url нашей ФС
URL = 'vpn.xamex.ru:22'
# Название приложения(Не понял зачем, но указать его надо!)
NAME_APP = 'app_task_5_1_RDD'


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
    sc: SparkContext = SparkContext(URL, NAME_APP)

    # Тут должен быть путь до файла с дампом твиттера, который надо закинуть в Spark (hdfs:///user/...)
    path_to_file = 'data/tweets.csv'
    lines: RDD = get_file_from_spark(sc, path_to_file)

    # Отрезаем заголовок в CSV файле, так как он не нужен
    header = lines.first()

    # Хуярим отфильтрованные с помощью лямбды строки.
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

    rp_tweet_time = re.compile('2015')
    rp_is_retweet = re.compile('TRUE')
    filtered: RDD = filter_file(records, lambda rec: rp_tweet_time.match(rec[tweet_time_index]) and
                                                     rp_is_retweet.match(rec[is_retweet_index]))

    result_tuples: RDD = map_file(filtered, lambda rec: (rec[tweet_text_index],
                                                         rec[tweet_time_index],
                                                         rec[is_retweet_index]))

    # max_temp: RDD = reduceByKey_obj(result_tuples, lambda a, b: a if a > b else b)
    save_as_text_file_obj(result_tuples, 'task_5_1_output')


if __name__ == '__main__':
    main()
