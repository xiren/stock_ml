import service.xueqiu_gateway as xq
import service.random_forest_classifier as rfc
import service.clean_data as cd

if __name__ == '__main__':
    xq.start()
    cd.start()
    rfc.start()
