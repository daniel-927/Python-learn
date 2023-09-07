import random
import time
import pysnooper
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
    "database": "filbet_dev_sharding"
}


@timer
@pysnooper.snoop()
def add_test_data():
    # 打开连接
    connection = pymysql.connect(**db_config)
    cursor = connection.cursor()

    # 开始循环查询
    # results = 0
    for i in range(1024):
        #sql = f"ALTER TABLE win_coin_log_{i} ADD COLUMN `merchant_id` int NOT NULL default 0 COMMENT '商户id' AFTER username;"
        #sql = f"ALTER TABLE win_betslips_{i} ADD COLUMN `merchant_id` int NOT NULL default 0 COMMENT '商户id' AFTER xb_username;"
        #sql = f"delete from win_betslips_{i} where id <3000;"
        #sql = f"truncate win_betslips_details_{i}"
        sql = f"show databases;"
        try:
            cursor.execute(sql)
            connection.commit()
            # result = cursor.fetchmany(1)
            # result = int(results[0][0])
            # results += result
            # print(results)
        except Exception as e:
            print(e)
    # print(results)
    connection.close()


add_test_data()
