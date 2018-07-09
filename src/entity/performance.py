class Performance:

    data_list = []

    def __init__(self, symbol, code, name):
        self.name = name
        self.code = code
        self.symbol = symbol

    def data_list(self, data_list):
        self.data_list = data_list
