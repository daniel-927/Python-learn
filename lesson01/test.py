import random
import time

import pymysql
import pymysql.cursors


# 装饰器 计算插入时间
def timer(func):
    def decor(*args):
        start_time = time.time()
        func(*args)
        end_time = time.time()
        d_time = end_time - start_time
        print("the running time is : ", d_time)

    return decor


db_config = {
    "host": "filbet-zi-dev-aurora-cluster.cluster-c0mmrepgi1ky.us-west-2.rds.amazonaws.com",
    "user": "admin",
    "password": "WATNfBJYaZ4FPVVzdYCq",
    "database": "filbet_sharding"
}


@timer
def add_test_data():
    # 打开连接
    connection = pymysql.connect(**db_config)
    cursor = connection.cursor()


    # 开始循环查询

    for i in range(1024):
        sql = f'delete  FROM win_betslips_{i} WHERE id <=150000 ;'
        try:
            cursor.execute(sql)
            connection.commit()
            #results = cursor.fetchmany(5)
            #print(results)
        except Exception as e:
            print(e)

    connection.close()


add_test_data()
