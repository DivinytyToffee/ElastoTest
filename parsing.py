import datetime
import io

from _io import TextIOWrapper


class DataParser:
    """
    Object load, parse and store data, loaded from file.
    """

    def __init__(self):
        self.__data = []
        self.__keys = ()

    def from_csv_file(self, file: TextIOWrapper, headers: bool = True):
        """
        Method make object from csv file.

        :param file:
        :param headers:
        :return:
        """
        read_file = file.read()
        splitted_data = read_file.split('\n')
        self.__make_keys_csv(splitted_data[0], headers)
        shift = 1 if headers else 0
        self.__data.extend(dict(zip(self.__keys, element.split(','))) for element in splitted_data[shift:])

    def column_min(self, column_name):
        """
        Method returns min element in column.

        :param column_name:
        :return:
        """
        return min(self.__aggregate_column_data(column_name))

    def column_max(self, column_name):
        """
        Method returns max element in column.

        :param column_name:
        :return:
        """
        return max(self.__aggregate_column_data(column_name))

    def column_descending_sort(self, column_name):
        """
        Method returns data sorted in descending order.

        :param column_name:
        :return:
        """
        data = sorted(self.__data, key=lambda elem: elem.get(column_name))
        return data

    def set_column_in_int(self, column_name):
        """
        Method to cast column data to int.

        :param column_name:
        :return:
        """
        self.__cast_to(column_name, int)

    def set_column_in_str(self, column_name):
        """
        Method to cast column data to str.

        :param column_name:
        :return:
        """
        self.__cast_to(column_name, str)

    def set_column_in_float(self, column_name):
        """
        Method to cast column data to float.

        :param column_name:
        :return:
        """
        self.__cast_to(column_name, int)

    def set_column_in_datetime(self, column_name):
        """
        Method to cast column data to datetime.

        :param column_name:
        :return:
        """
        self.__cast_to(column_name, datetime.datetime.strptime, format='%Y-%m-%d')

    def set_column_in_date(self, column_name):
        """
        Method to cast column data to date.

        :param column_name:
        :return:
        """
        def date(obj, **kwargs):
            return datetime.datetime.strptime(obj, kwargs.get('format')).date()

        self.__cast_to(column_name, date, format='%Y-%m-%d')

    def clear(self):
        """
        Method clear all data.

        :return:
        """
        self.__data.clear()
        self.__keys = ()

    def __cast_to(self, column_name, cats_func, **kwargs):
        """
        Common method for cast values in object to some type.

        :param column_name:
        :param cats_func:
        :param kwargs: addition arguments for cast function
        :return:
        """
        for elem in self.__data:
            cast = cats_func(elem.get(column_name), **kwargs)
            elem.update({column_name: cast})

    def __make_keys_csv(self, element: str, headers: bool):
        """
        Method makes keys for object from csv file.

        :param element: string table element separated ','
        :param headers: boolean flag for check existing columns headers
        :return: tuple with keys
        """
        if headers:
            self.__keys = tuple(element.split(','))
        else:
            self.__keys = tuple(x for x in range(1, len(element.split(',')) + 1))

    def __aggregate_column_data(self, column_name):
        """
        Method for collecting and returning data by column name.

        :param column_name:
        :return:
        """
        return (x.get(column_name) for x in self.__data)

    @property
    def data(self):
        """
        Property return loaded and parsed data.

        :return:
        """
        return self.__data

    @property
    def keys(self):
        """
        Property return keys to loaded and parsed data.

        :return:
        """
        return self.__keys


if __name__ == '__main__':
    file_ = io.open("datasource.csv", mode="r", encoding="utf-8")
    # file_ = open('', 'r')
    a = DataParser()
    a.from_csv_file(file_)
    # a.set_column_in_date('Дата продажи')
    # a.set_column_in_float('Стоимость товара')
    # a.set_column_in_int('Количество товара')
    for x, y in zip(a.column_descending_sort('Стоимость товара'), a.data):
        print(f'{x}::{y}')

    print(a.column_max('Стоимость товара'))
    print(a.column_min('Стоимость товара'))
