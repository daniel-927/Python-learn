# /usr/bin/python3

# Author        : Ives
# Date          : 2024-08-07

import datetime
import subprocess

def manage_db_partitions(db_host, db_user, db_pwd, db_list, tables_list):
    """
    管理数据库分区的函数。

    参数:
    db_host (str): 数据库主机地址
    db_user (str): 数据库用户名
    db_pwd (str): 数据库密码
    db_list (list): 库列表
    tables_list (list): 分区表列表
    """
    current_date = datetime.datetime.now()

    # 循环递增7天 (即新增7天后的7-14天分区, 删除30天前的30-37天分区)
    for i in range(8):
        next_week = current_date + datetime.timedelta(days=7 + i)
        last_30_days = current_date - datetime.timedelta(days=30 - i)
        target_time = datetime.time(1, 0, 0)

        next_target_datetime = datetime.datetime.combine(next_week.date(), target_time)
        last_target_datetime = datetime.datetime.combine(last_30_days.date(), target_time)

        next_week_timestamp = int(next_target_datetime.timestamp() * 1000)
        last_30_days_timestamp = int(last_target_datetime.timestamp() * 1000)

        year_str_next = str(next_week.year)
        month_str_next = str(next_week.month).zfill(2)
        day_str_next = str(next_week.day).zfill(2)
        date_str_next = "p" + year_str_next + month_str_next + day_str_next

        year_str_last = str(last_30_days.year)
        month_str_last = str(last_30_days.month).zfill(2)
        day_str_last = str(last_30_days.day).zfill(2)
        date_str_last = "p" + year_str_last + month_str_last + day_str_last

        for dbs in db_list:
            for tbs in tables_list:
                # 检查是否存在要删除的分区
                check_drop_exists = f"SELECT 1 FROM information_schema.partitions WHERE table_schema = '{dbs}' AND table_name = '{tbs}' AND partition_name = '{date_str_last}'"
                cmd_check_drop_exists = f"mysql -h{db_host} -u{db_user} -p{db_pwd} -e \"{check_drop_exists}\""
                result_drop_exists = subprocess.run(cmd_check_drop_exists, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

                if "1" in result_drop_exists.stdout.decode('utf-8'):
                    # 删除30天前的分区
                    sql_drop = f'use {dbs};ALTER TABLE {tbs} DROP PARTITION {date_str_last}'
                    cmd_drop = f"mysql -h{db_host}  -u{db_user} -p{db_pwd} -e '{sql_drop}'"
                    subprocess.run(cmd_drop, shell=True, check=True)
                    print(f"Deleted partition {date_str_last} for table {tbs}")
                else:
                    print(f"Partition {date_str_last} does not exist for table {tbs}, skipping deletion")

                # 检查是否存在要添加的分区
                check_add_exists = f"SELECT 1 FROM information_schema.partitions WHERE table_schema = '{dbs}' AND table_name = '{tbs}' AND partition_name = '{date_str_next}'"
                cmd_check_add_exists = f"mysql -h{db_host} -u{db_user} -p{db_pwd} -e \"{check_add_exists}\""
                result_add_exists = subprocess.run(cmd_check_add_exists, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

                if "1" not in result_add_exists.stdout.decode('utf-8'):
                    # 添加七天后的分区
                    sql_add = f'use {dbs};ALTER TABLE {tbs} ADD PARTITION (partition {date_str_next} values less than ({next_week_timestamp}))'
                    cmd_add = f"mysql -h{db_host} -u{db_user} -p{db_pwd} -e '{sql_add}'"
                    subprocess.run(cmd_add, shell=True, check=True)
                    print(f"Added partition {date_str_next} for table {tbs}")
                else:
                    print(f"Partition {date_str_next} already exists for table {tbs}, skipping addition")

# 使用示例

# 分区表列表
tables_list = [
    'tab_financialchess',
    'tab_financialelectronic',
    'tab_financialelectronic_jili',
    'tab_financialelectronic_pg',
    'tab_financialelectronic_pp',
    'tab_financialelectronic_spribe',
    'tab_financialelectronic_tb',
    'tab_financiallottery_5d',
    'tab_financiallottery_k3',
    'tab_financiallottery_trxwingo',
    'tab_financiallottery_wingo',
    'tab_financialsport',
    'tab_financialtenant',
    'tab_financialvideo',
    'tab_gameusers',
    'tab_orderchess',
    'tab_orderelectronic',
    'tab_orderelectronic_jili',
    'tab_orderelectronic_pg',
    'tab_orderelectronic_pp',
    'tab_orderelectronic_spribe',
    'tab_orderelectronic_tb',
    'tab_orderlottery_5d',
    'tab_orderlottery_k3',
    'tab_orderlottery_trxwingo',
    'tab_orderlottery_wingo',
    'tab_ordersport',
    'tab_ordervideo',
    'tab_tenanttransfer'
]

# 实例1
db_host = "ar0607.rwlb.singapore.rds.aliyuncs.com"
db_user = "ives"
db_pwd = "uNv5h7QUkUe9!AiBEFNpDWF7Nf3L6j"
db_list = ['tenant_1001']

manage_db_partitions(db_host, db_user, db_pwd, db_list, tables_list)


# 实例2
db_host = "sit-tenat.rwlb.singapore.rds.aliyuncs.com"
db_user = "sa"
db_pwd = "g3AGKKc!8XbQiRzaW2upgNHHAz"
db_list = ['tenant_2001','tenant_2002']

manage_db_partitions(db_host, db_user, db_pwd, db_list, tables_list)
