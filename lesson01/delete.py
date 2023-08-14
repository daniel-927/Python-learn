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
for i in range(1024):
    delete = f"DELETE FROM win_betslips_{i} WHERE id > 69999 and id < 130001;"
    print(cursor.execute(delete))
connection.commit()
connection.close()