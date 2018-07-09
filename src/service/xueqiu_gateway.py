import datetime
import json
import os
import time
import urllib.request
from multiprocessing.pool import Pool

from entity.performance import Performance

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:57.0) Gecko/20100101 Firefox/57.0',
    'Connection': 'keep-alive'
}


def get_data_by_symbol(symbol):
    init_cookie()
    request = urllib.request.Request(
        'https://xueqiu.com/stock/forchartk/stocklist.json?symbol={0}&period=1day&type=before&begin=1900-01-01&end={1}'.format(
            symbol, datetime.date.today()),
        headers=headers)
    response = urllib.request.urlopen(request)
    response_json = json.loads(response.read().decode('UTF-8'))
    for r in response_json['chartlist']:
        r['time'] = time.strftime('%Y-%m-%d', time.strptime(r['time'], '%a %b %d %H:%M:%S %z %Y'))
    return response_json['chartlist']


inited = False


def init_cookie():
    global inited
    if not inited:
        request = urllib.request.Request('https://xueqiu.com', headers=headers)
        response = urllib.request.urlopen(request)
        cookie = response.getheader('Set-Cookie')
        cookie_str = ''
        for item in cookie.split(';'):
            if '=' in item:
                k, v = item.split('=')
                if ',' in k:
                    key = k.split(',')[1].strip()
                else:
                    key = k.strip()
                value = v
                if 'token' in key or 'u' == key or 'aliyun' in key:
                    cookie_str = cookie_str + key + "=" + value + "; "
        headers['Cookie'] = cookie_str
        inited = True


def get_stock_list():
    init_cookie()
    page = 1
    size = 30
    stock_list_url = 'https://xueqiu.com/stock/cata/stocklist.json?page={0}&size={1}&order=desc&orderby=percent&type=11'
    request = urllib.request.Request(stock_list_url.format(page, size), headers=headers)
    response = urllib.request.urlopen(request)
    response_json = json.loads(response.read().decode('UTF-8'))
    tuple_list = []
    for st in response_json['stocks']:
        tuple_list.append((st['symbol'], st['code'], st['name']))
    count = response_json['count']['count']
    while count - size * page > 0:
        page = page + 1
        tuple_list = tuple_list + parse_response(stock_list_url.format(page, size))
    return tuple_list


def parse_response(stock_list_url):
    request = urllib.request.Request(stock_list_url, headers=headers)
    response = urllib.request.urlopen(request)
    response_json = json.loads(response.read().decode('UTF-8'))
    tuple_list = []
    for st in response_json['stocks']:
        tuple_list.append((st['symbol'], st['code'], st['name']))
    return tuple_list


def work(stock):
    data_list = get_data_by_symbol(stock[0])
    performance = Performance(stock[0], stock[1], stock[2])
    performance.data_list(data_list)
    if len(data_list) != 0:
        print(stock[0], stock[1], stock[2])
        path = '../../data/raw_data/code=' + stock[0]
        if not os.path.exists(path):
            os.makedirs(path)
        with open(path + '/' + stock[0] + '.json', 'wb') as f:
            f.write(json.dumps(performance.data_list).encode('UTF-8'))


def clean_folder(path, boolean=False):
    if os.path.isfile(path):
        os.remove(path)
    elif os.path.isdir(path):
        for sub_path in os.listdir(path):
            clean_folder(os.path.join(path, sub_path), True)
        if len(os.listdir(path)) == 0 and boolean:
            os.rmdir(path)


def start():
    clean_folder('../../data/raw_data')
    stock_list = get_stock_list()
    pool = Pool(10)
    pool.map(work, stock_list)
    pool.close()
    pool.join()
    print('finished')


if __name__ == '__main__':
    start()
