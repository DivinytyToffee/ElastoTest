# Запуск
Установить зависимости из `req.txt`

Запустить файл командой `python main.py`

#### Задать файл
Задать файл для парсинга POST запросом `localhost:8080/set_data`, файл передаётся в json `{"path": path_to_file}`

**Файл должен быть в `csv` формате**

#### Получение данных
Для получения данных требуется GET запрос на `localhos:8080/get_data`

##### Минимум в столбце
`column_min=<Имя столбца>`

##### Максимум в столбце
`column_max=<Имя столбца>`

##### Сортировка по убыванию
`descending=<Имя столбца>`

##### Привести к типу
Для приведения конлки к конкретному типу требуется POST запрос на `localhost:8080/cast_to`, в запрос надо добавить json 
`{"int": [<Названия колонок>], "float": [<Названия колонок>], "date": [<Названия колонок>], "datetime": [<Названия колонок>]}` 