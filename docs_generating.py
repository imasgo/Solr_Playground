from db_creation import *
from itertools import combinations, chain, product
from lib import logging, report, query


def get_cube_name(cube_id):
    for c in Cube.raw('select name from cube where id = %s' % cube_id):
        return c.name


def get_cube_measures_dimensions(cube_id):
    md = {}
    for m in Cube_Value.raw('select value_id from cube_value where cube_id = %s' % cube_id):
        for v in Value.raw('select nvalue, fvalue from value where id = %s' % m.value_id):
            md[m.value_id] = (v.nvalue, v.fvalue)

    dd = {}
    for cd in Cube_Dimension.raw('select dimension_id from cube_dimension where cube_id = %s' % cube_id):
        for d in Dimension.raw('select label from dimension where id = %s' % cd.dimension_id):
            dd[cd.dimension_id] = d.label

    return md, dd


def get_all_dimension_combinations(dd, cube_id, clever=True):
    """если clever==True, то комбинации составляются на основе БД (см. таблицу combinations)"""

    dimension_sets = []  # все возможные сочетания измерений
    if not clever:
        for i in range(1, len(dd) + 1):
            l = list(combinations(dd.keys(), i))
            dimension_sets.append(l)

        dimension_sets = list(chain(*dimension_sets))  # уплощение листа
    else:
        # TODO: 3NF в БД?
        for comb in Combination.raw('select combination from combination where cube_id = %s' % cube_id):
            dimension_sets.append(tuple(map(int, comb.combination.split())))

    return dimension_sets


def get_all_possible_combinations(md, dd, d_sets):
    log_1st_step = ''
    full_sets = []

    for m in md:
        for idx, d_set in enumerate(d_sets):
            d = ''
            for elem in d_set:
                d += dd[elem] + ' '
            log_1st_step += '{}. M:{} D:{}\n'.format(idx, md[m][1], d)
            full_sets.append([m, d_set])

    logging('1st step', log_1st_step)

    return full_sets


def docs_needed(md, dd, d_sets):
    dim_count = {}
    for d in dd:
        count = Dimension_Value.raw('select count(*) from dimension_value where dimension_id = %s' % d).scalar()

        if count == 0:
            count = 1

        dim_count[d] = count

    res = 0
    for item in d_sets:
        r = 1
        for elem in item:
            r *= dim_count[elem]
        res += r

    res *= len(md)

    return dim_count, res


def generate_documents(mdict, ddict, full_sets, cube_name):
    """Генерация документов"""

    # выбор всех измерений со значениями
    dim_vals = []
    for idx, d in enumerate(ddict):
        dim_vals.append([])
        dim_vals[idx].append(d)
        dim_vals[idx].append([])
        for dv in Dimension_Value.raw('select value_id from dimension_value where dimension_id = %s' % d):
            for v in Value.raw('select fvalue, nvalue from value where id = %s' % dv.value_id):
                dim_vals[idx][1].append((v.nvalue, v.fvalue))

    docs = []
    mdx_template = 'SELECT {} ON COLUMNS FROM [{}.DB] WHERE ({})'

    # TODO: рефакторить код
    for idx, set in enumerate(full_sets):
        # если мы используем только одно измерение
        if len(set[1]) == 1:
            for dim_val in dim_vals:
                if dim_val[0] == set[1][0]:
                    for dim_v in dim_val[1]:
                        fr = mdx_template.format('{[MEASURES].[' + mdict[set[0]][1] + ']}', cube_name,
                                                 '[{}].[{}]'.format(ddict[set[1][0]], dim_v[1]))
                        nr = mdict[set[0]][0] + ' ' + dim_v[0]
                        docs.append([fr, nr])
        else:
            # массив со всеми (за некоторыми исключениями) значениями набора измерений
            l = []

            # проверка наличия измерения территория в наборе
            territory_filter = False
            for d_id in set[1]:
                if ddict[d_id] == 'TERRITORIES':
                    territory_filter = True
                    break

            for d_id in set[1]:
                for dim_val in dim_vals:
                    if dim_val[0] == d_id:
                        # если в наборе есть территория и сейчас идет итерация по измерению BGLEVELS
                        if ddict[d_id] == 'BGLEVELS' and territory_filter is True:
                            l.append([('бюджет субъекта', '09-3')])
                        else:
                            l.append(dim_val[1])
                        break

            combs = list(product(*l))
            q = 1
            f = open("C:\\Users\\The Cat Trex\\Desktop\\solr-6.3.0\\example\\exampledocs\\newdata.txt", 'w')

            for item in combs:
                s, k = '', ''

                for elem, d_id in zip(item, set[1]):
                    s += '[{}].[{}],'.format(ddict[d_id], elem[1])
                    k += elem[0] + ' '

                s, k = s[:-1], k[:-1]

                fr = mdx_template.format('{[MEASURES].[' + mdict[set[0]][1] + ']}', cube_name, s)
                nr = mdict[set[0]][0] + ' ' + k
                docs.append([fr, nr])
                print(len(docs))
                
    print('Документов создалось: {}'.format(len(docs)))
    j = 0
    for q in docs:
        if j == 1:
            json_string = '[{"ID":"' + str(j) + '","MDX_Request":"' + q[0] + '","Normal_request":"' + q[1] + '"}'
            f.write(json_string)
        else:
            json_string = ',{"ID":"' + str(j) + '","MDX_Request":"' + q[0] + '","Normal_request":"' + q[1] + '"}'
            f.write(json_string)
        j += 1

    f.write(']')
    f.close()
    return docs


def learn_model(docs):
    """Отсеивание нерабочих документов"""

    new_docs = []
    for item in docs:
        if query(item[0]):
            new_docs.append(item)
    print('Документов осталось: {}'.format(len(new_docs)))
    return new_docs

id_cube = 1
name_cube = get_cube_name(id_cube)
mdict, ddict = get_cube_measures_dimensions(id_cube)
dim_sets = get_all_dimension_combinations(ddict, id_cube)
measure_dim_sets = get_all_possible_combinations(mdict, ddict, dim_sets)
dim_count, doc_count = docs_needed(mdict, ddict, dim_sets)

# генерация документов
documents = generate_documents(mdict, ddict, measure_dim_sets, name_cube)

# обучение путем проверки корректности документа, через реальный запрос к серверу
# !ОСТОРОЖНО!
# only_working_documents = learn_model(documents)
# !ОСТОРОЖНО!

report(id_cube, name_cube, mdict, ddict, dim_sets, measure_dim_sets, dim_count, doc_count)
