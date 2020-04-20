import datetime
import json
import io

from bottle import run, get, post, request, response

from parsing import DataParser


DATAPARSER = DataParser()
DATAFILE = 'datasource.csv'


def myconverter(o):
    """
    Custom serializer for date's.

    :param o:
    :return:
    """
    if isinstance(o, datetime.datetime):
        return o.__str__()

    if isinstance(o, datetime.date):
        return o.__str__()


@get('/get_data')
def get_data():
    ret_obj = {}

    if not DATAPARSER.data:
        response.status = 523
        return 'No data. 523'

    column_min = not isinstance(request.query.get('column_min'), type(None))
    column_max = not isinstance(request.query.get('column_max'), type(None))
    descending = not isinstance(request.query.get('descending'), type(None))

    try:
        if column_min:
            value = DATAPARSER.column_min(request.query.column_min)
            ret_obj.update({'min': {type(value).__name__:  value}})

        if column_max:
            value = DATAPARSER.column_max(request.query.column_max)
            ret_obj.update({'max': {type(value).__name__:  value}})

        if descending:
            value = DATAPARSER.column_descending_sort(request.query.descending)
            ret_obj.update({'descending_sort':  value})

        if not (column_min or column_max or descending):
            ret_obj = DATAPARSER.data
    except Exception as ex:
        response.status = 400
        return json.dumps({'status': 'Error 400', 'message': ex.__str__()})

    ret = json.dumps(ret_obj, ensure_ascii=False, default=myconverter).encode('utf8')
    response.status = 200
    return ret


@post('/set_data')
def set_data():
    file = request.json.get('path')
    if len(DATAPARSER.data) > 0:
        DATAPARSER.clear()

    if file:
        if not file.endswith('.csv'):
            response.status = 400
            return {'status': 'Error', 'message': f'file {file} is not csv'}
        try:
            file_ = io.open(file, mode="r", encoding="utf-8")
            DATAPARSER.from_csv_file(file_)
            response.status = 200
            return {'status': 'ok', 'message': f'file {file} is parsed'}

        except:
            response.status = 400
            return {'status': 'Error', 'message': 'Failed to parse'}
    else:
        response.status = 400
        return {'status': 'Error', 'message': 'Not path to file'}


@post('/cast_to')
def cast_to():
    int_cast = not isinstance(request.json.get('int'), type(None))
    float_cast = not isinstance(request.json.get('float'), type(None))
    date_cast = not isinstance(request.json.get('date'), type(None))
    datetime_cast = not isinstance(request.json.get('datetime'), type(None))
    try:
        if int_cast:
            for x in request.json.get('int'):
                DATAPARSER.set_column_in_int(x)

        if float_cast:
            for x in request.json.get('float'):
                DATAPARSER.set_column_in_float(x)

        if date_cast:
            for x in request.json.get('date'):
                DATAPARSER.set_column_in_date(x)

        if datetime_cast:
            for x in request.json.get('datetime'):
                DATAPARSER.set_column_in_datetime(x)

    except Exception as ex:
        return {'status': 'error', 'message': f'Error in cast {ex.__str__()}'}

    return {'status': 'ok', 'message': 'All cast is correct'}


if __name__ == '__main__':
    run(host='localhost', port=8080)
