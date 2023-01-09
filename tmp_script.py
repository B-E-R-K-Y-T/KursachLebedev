# запустить можно с помощью команды, поменяв {username} на имя своего пользователя(Я ещё добавил 3-й аргумент: path):
# python3 consultant_for_pairs.py localhost 50070 {username} {path}
import contextlib
import sys
import pyhdfs
import ast
from collections import Counter

# server = sys.argv[1]
# port = sys.argv[2]
# username = sys.argv[3]
#
# path = f"/user/{username}/laba2_2/output"
# file_name = "/part-00000"

server = sys.argv[1]
port = sys.argv[2]
username = sys.argv[3]
# если будут ошибки, то они могут быть из-за этой строки
path = sys.argv[4] if len(sys.argv) > 3 else f"/user/{username}/laba2_1/output"
file_name = "/part-00000"


def consultant():
    while True:
        print(
            "**********************\nCommands:\n1. exit_ - exit\n2. \"word\" - your word for serching recommendations" +
            "\n**********************\n")
        command = input('enter your command: ')
        if command == "exit_":
            break
        print(f"\nHello, {username}! We can advise you:")
        fs = pyhdfs.HdfsClient(hosts=f'{server}:{port}', user_name=username)
        with contextlib.closing(fs.open(f'{path}{file_name}')) as f:
            for line in f:
                string_from_doc = line.decode().strip().split('\t')[1]
                from_str_to_dict = ast.literal_eval(string_from_doc)
                pairs_from_dict = dict(from_str_to_dict).items()
                sorted_values = sorted(pairs_from_dict, key=lambda item: item[1])
                dicti = dict(sorted_values)
                dicti = dict(sorted(dicti.items(), key=lambda item: item[1], reverse=True))
                if command in str(line.decode().strip().split('\t')[0]):
                    if len(dicti) >= 10:
                        temp = 0
                        for key in dicti:
                            if temp < 10:
                                print(key)
                                temp += 1
                            else:
                                break
                    else:
                        for item in dicti:
                            print(item)


consultant()