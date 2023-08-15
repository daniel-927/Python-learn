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

    # 定义字段
    num_entries = 3
    table_id = 0
    xb_status = 1
    xb_uid = 23552
    xb_username = '"orange86"'
    stake = 100.0000
    valid_stake = 100.0000
    payout = 0.0000
    coin_refund = 0.0000
    coin_before = 0.0000
    game_provider_subtype_id = 205
    game_list_id = 13581
    game_pagcor_id = 1
    game_type_id = 2
    game_provider_id = 6
    amount_type = 1
    dt_started = 1691400658
    dt_completed = 1691400658
    win_transaction_id = 1
    create_time_str = '2023-08-07'
    created_at = 1691400658
    updated_at = 1691400658

    # 开始循环插入
    for num_id in range(num_entries):
        table_id += 1
        round_id = f"{random.randint(1, 100000000)}"
        transaction_id = f"{random.randint(1, 100000000)}"
        xb_profit = f"{random.randint(1, 1000)}"

        for i in range(1024):
            table_id += 1
            values24 = [({table_id}, {round_id}, {transaction_id}, {xb_status}, {xb_uid}, {xb_username}, {xb_profit},
                         {stake}, {valid_stake}, {payout}, {coin_refund}, {coin_before}, {game_provider_subtype_id},
                         {game_list_id}, {game_pagcor_id}, {game_type_id}, {game_provider_id}, {amount_type},
                         {dt_started},
                         {dt_completed}, {win_transaction_id}, {create_time_str}, {created_at}, {updated_at})]
            try:
                cursor.executemany(f'insert into win_betslips_{i} (id,round_id,transaction_id,xb_status,xb_uid,xb_username,xb_profit,stake,valid_stake,payout,coin_refund,coin_before,game_provider_subtype_id,game_list_id,game_pagcor_id,game_type_id,game_provider_id,amount_type,dt_started,dt_completed,win_transaction_id,create_time_str,created_at,updated_at) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',values24)
            except Exception as e:
                print(e)
        connection.commit()
    connection.close()


add_test_data()
