import random
import time
import pymysql
import pymysql.cursors

db_config = {
    "host": "filbet-zi-dev-aurora-cluster.cluster-c0mmrepgi1ky.us-west-2.rds.amazonaws.com",
    "user": "admin",
    "password": "WATNfBJYaZ4FPVVzdYCq",
    "database": "filbet_sharding"
}

# 数据库连接
connection = pymysql.connect(**db_config)
cursor = connection.cursor()

# 开始循环
num_entries = 1
table_id = 0
for num_id in range(num_entries):
    table_id += 1
    round_id = f"{random.randint(1, 100000000)}"
    transaction_id = f"{random.randint(1, 100000000)}"
    xb_status = 1
    xb_uid = 23552
    xb_username = '"orange86"'
    xb_profit = f"{random.randint(1, 1000)}"
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

    # 构造SQL插入语句
    for i in range(1024):
        # insert_query = f"insert into win_betslips_{i} (id,round_id,transaction_id,xb_status,xb_uid,xb_username,xb_profit,stake,valid_stake,payout,coin_refund,coin_before,game_provider_subtype_id,game_list_id,game_pagcor_id,game_type_id,game_provider_id,amount_type,dt_started,dt_completed,win_transaction_id,create_time_str,created_at,updated_at)values({table_id},{round_id},{transaction_id},{xb_status},{xb_uid},{xb_username},{xb_profit},{stake},{valid_stake},{payout},{coin_refund},{coin_before},{game_provider_subtype_id},{game_list_id},{game_pagcor_id},{game_type_id},{game_provider_id},{amount_type},{dt_started},{dt_completed},{win_transaction_id},{create_time_str},{created_at},{updated_at});"
        delete = f"DELETE FROM win_betslips_{i} WHERE id ={table_id};"
        print(cursor.execute(delete))
connection.commit()
connection.close()
