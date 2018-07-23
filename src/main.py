import service.xueqiu_gateway as xq
import service.predict_data as p
import service.clean_data as c

if __name__ == '__main__':
    xq.start()
    c.start()
    p.start()
