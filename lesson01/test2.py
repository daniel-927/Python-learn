import random
import time

import pymysql
import pymysql.cursors


# 装饰器 计算时间
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
    "database": "test"
}


@timer
def add_test_data():
    # 打开连接
    connection = pymysql.connect(**db_config)
    cursor = connection.cursor()



    # 获取总行数
    totalrows_sql = f'SELECT COUNT(1)  FROM 2_win_betslips WHERE game_list_id = 0;'
    cursor.execute(totalrows_sql)
    totalrows = cursor.fetchmany(1)
    totalrows = int(totalrows[0][0])

    # 开始循环查询
    while totalrows > 0:
        count1=1
        try:        
            temprows = 1000
            sql = f'UPDATE 2_win_betslips a	JOIN (SELECT id FROM 2_win_betslips WHERE game_list_id = 0 LIMIT 1000) c ON a.id = c.id LEFT JOIN game_list b ON a.game_type_id = b.code AND a.game_plat_id = b.game_provider_id AND a.game_id = b.game_provider_subtype_id  SET a.game_list_id = b.id, a.game_provider_id = b.game_provider_id, a.game_pagcor_id = b.game_pagcor_id, a.game_provider_subtype_id = b.game_provider_subtype_id  WHERE a.game_list_id=0; '
            cursor.execute(sql)
            totalrows = totalrows - temprows
            connection.commit()

            
        except Exception as e:
            print(e)
        count1 += 1
    connection.close()


add_test_data()
