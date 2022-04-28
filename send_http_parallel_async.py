import gevent.monkey
gevent.monkey.patch_all()

import requests
import grequests

df = spark.createDataFrame(
    lista_dados,
    ["id", "label"]
)

lista_dados = []
for x in range(100000):
    data = (x, "foo_" + str(x))
    lista_dados.append(data)



hostname = 'https://jsonplaceholder.typicode.com/posts'


def get_data(df):
    start_date = datetime.now()
    d = df.collect()
    end_date = datetime.now()
    total_time = (end_date - start_date).total_seconds()
    
    return d, total_time


def create_map(partitionData):
    def error_handler(request, exception):
        request_data = request.kwargs
        data = request_data['data']
        data['result'] = {'timeout': 1}
        return data
    reqs = (
        grequests.post(
            hostname,
            data={
                'title': row.label,
                'body': 'bar',
                'userId': row.id
            },
            timeout=1
        ) for row in partitionData
    )
    return grequests.map(reqs, size=10, exception_handler=error_handler)



def get_response(response):
    def get_request(response):
        request_data = response.request.body.replace('&', ',')
        data = dict(item.split("=") for item in request_data.split(","))
        return data
    result = {}
    if response and not isinstance(response, dict):
        if response.status_code == 201:
            result = get_request(response)
            result['result'] = {'success': 1}
        if response.status_code != 201:
            result = get_request(response)
            result['result'] = {'error': 1}
    else:
        result = response
    return result


df_repartition = df.repartition(2000)
df2 = df_repartition.rdd.mapPartitions(create_map).map(get_response)

d1, d2 = get_data(df2)

df_final = df2.map(lambda line: Row(**line)).toDF()
