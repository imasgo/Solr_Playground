import requests
import json


def query(q):
    query_by_elements = q.split(' ')
    from_element = query_by_elements[query_by_elements.index('FROM') + 1]
    cube = from_element[1:len(from_element) - 4]

    d = {'dataMartCode': cube, 'mdxQuery': q}
    r = requests.post('http://conf.test.fm.epbs.ru/mdxexpert/CellsetByMdx', d)
    t = json.loads(r.text)
    # print(t)

    if 'success' in t or t["cells"][0][0]["value"] is None:
        return False
    else:
        return True


def logging(file_name, text):
    with open(file_name + '.txt', 'w') as file:
        file.write(text)


def report(cube_id, cube_name, md, dd, d_sets, full_sets, dim_num, doc_num):
    print('Куб: {}, cube_id: {}'.format(cube_name, cube_id))
    print('=' * 10 + ' Шаг 1 ' + '=' * 10)
    print('Меры: {}шт. {}\nИзмерения: {}шт. {}'.format(len(md), md, len(dd), dd))
    print('=' * 10 + ' Шаг 2 ' + '=' * 10)
    print('Количество сочетаний измерений: %s' % len(d_sets))
    # print(d_sets)
    print('=' * 10 + ' Шаг 3 ' + '=' * 10)
    print('Количество сочетаний измерений и мер: %s' % len(full_sets))
    print(full_sets)
    print('=' * 10 + ' Шаг 4 ' + '=' * 10)
    print('(Измерение : Количество значений)')
    for key, value in dim_num.items():
        print('({}:{})'.format(dd[key], value))
    print('Полное количество документов до удаления невозможных запросов без фильтров: {:,}'.format(doc_num))


# query('SELECT {[MEASURES].[VALUE]} ON COLUMNS FROM [EXDO01.DB] WHERE ([TERRITORIES].[08-67724], [BGLEVELS].[09-3], [RZPR].[14-848223], [MARKS].[03-4])')
# query('SELECT {[MEASURES].[VALUE]} ON COLUMNS FROM [EXDO01.DB] WHERE ([TERRITORIES].[08-67724], [BGLEVELS].[09-8])')
# query('SELECT {[MEASURES].[VALUE]} ON COLUMNS FROM [EXDO01.DB] WHERE ([TERRITORIES].[08-67724], [BGLEVELS].[09-3], [KVR].[16-848369])')