import json
import os
from multiprocessing.pool import Pool


def avg(*arg1):
    return sum(arg1) / len(arg1)


def rate(arg1, arg2, arg3):
    if arg3 == 0.0:
        return 1.0
    return (arg1 - arg2) / arg3


def rebuild(arg):

    return {'col1': rate(arg['high'], arg['open'], arg['open']),
            'col2': rate(arg['open'], arg['low'], arg['open']),
            'col3': rate(arg['high'], arg['close'], arg['close']),
            'col4': rate(arg['close'], arg['low'], arg['close']),
            'col5': arg['chg'],
            'col6': arg['macd'],
            'col7': arg['dif'],
            'col8': arg['dea']}


def clean_folder(path, boolean=False):
    if os.path.isfile(path):
        os.remove(path)
    elif os.path.isdir(path):
        for sub_path in os.listdir(path):
            clean_folder(os.path.join(path, sub_path), True)
        if len(os.listdir(path)) == 0 and boolean:
            os.rmdir(path)


def is_expected(avg_close, sub_col_rdd, i):
    for x in range(10):
        if (x + i) < len(sub_col_rdd):
            if (1.1 * avg_close) <= sub_col_rdd[i + x]['close']:
                return True
        else:
            return False
    return False


def work(sub_col_rdd, code):
    col_len = len(sub_col_rdd)
    target_data = []
    for i in range(col_len):
        if i + 4 < col_len:
            first = sub_col_rdd[i]
            second = sub_col_rdd[i + 1]
            third = sub_col_rdd[i + 2]
            # avg_close = avg(first['close'], second['close'], third['close'])
            label = 0.0
            if is_expected(third['close'], sub_col_rdd, i + 4):
                label = 1.0
            obj = {'index': str(i) + '-' + code,
                   'first': rebuild(first),
                   'second': rebuild(second),
                   'third': rebuild(third),
                   'label': label}
            target_data.append(obj)

    print('print file', code)
    clean_data_path = '../../data/clean_data/code=' + code
    if not os.path.exists(clean_data_path):
        os.makedirs(clean_data_path)
    with open(clean_data_path + '/' + code + '.json', 'wb') as f:
        f.write(json.dumps(target_data).encode('UTF-8'))
    latest_1 = sub_col_rdd[col_len - 1]
    latest_2 = sub_col_rdd[col_len - 2]
    latest_3 = sub_col_rdd[col_len - 3]
    bid = avg(latest_1['close'], latest_2['close'], latest_3['close'])
    test_obj = {'index': str(i) + '-' + code,
                'first': rebuild(latest_3),
                'second': rebuild(latest_2),
                'third': rebuild(latest_1),
                'bid': latest_1['close'],
                'target': 1.1 * latest_1['close']}

    test_data_path = '../../data/test_data/code=' + code
    if not os.path.exists(test_data_path):
        os.makedirs(test_data_path)
    with open(test_data_path + '/' + code + '.json', 'wb') as f:
        f.write(json.dumps(test_obj).encode('UTF-8'))


def start():
    clean_folder('../../data/clean_data')
    clean_folder('../../data/test_data')
    pool = Pool(50)
    for root, dirs, files in os.walk('../../data/raw_data'):
        for dir in dirs:
            data_code = dir.split('=')[1]
            sub_folder = os.path.join(root, dir)
            for file in os.listdir(sub_folder):
                with open(os.path.join(sub_folder, file)) as raw_data_file:
                    raw_list_json = json.loads(raw_data_file.readline())
                    pool.apply_async(work, args=(raw_list_json, data_code))

    pool.close()
    pool.join()
    print('finish')


if __name__ == '__main__':
    start()
