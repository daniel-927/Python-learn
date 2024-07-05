# /usr/bin/python3

# Author        : Ives
# Date          : 2024-07-05
import datetime
import subprocess


# 每天定时执行即可
db_list = [
    'tenant_1001',
    'tenant_1002'
]

tables_list = [
    'tab_financialchess',
    'tab_financialelectronic',
    'tab_tenanttransfer'
]

# 获取当前日期和时间
current_date = datetime.datetime.now()


# 循环递增7天 (即新增7天后的7-14天分区,删除30天前的30-37天分区)
for i in range(8):


    # 计算七天后的日期
    next_week = current_date + datetime.timedelta(days=7+i)

    # 计算30天前的日期
    last_30_days = current_date - datetime.timedelta(days=30-i)

    # 设置每天凌晨1点的时间
    target_time = datetime.time(1, 0, 0)

    # 组合日期和时间
    next_target_datetime = datetime.datetime.combine(next_week.date(), target_time)
    last_target_datetime = datetime.datetime.combine(last_30_days.date(), target_time)

    # 七天后的时间戳
    next_week_timestamp = int(next_target_datetime.timestamp() * 1000)

    # 30天前的时间戳
    last_30_days_timestamp = int(last_target_datetime.timestamp() * 1000)
    
    # 获取七天后的日期的年、月、日
    year_str_next = str(next_week.year)
    month_str_next = str(next_week.month).zfill(2)
    day_str_next = str(next_week.day).zfill(2)

    date_str_next = "p" + year_str_next + month_str_next + day_str_next

    # 获取30天前的日期的年、月、日
    year_str_last = str(last_30_days.year)
    month_str_last = str(last_30_days.month).zfill(2)
    day_str_last = str(last_30_days.day).zfill(2)

    date_str_last = "p" + year_str_last + month_str_last + day_str_last
    for dbs in db_list:
        for tbs in tables_list:
            # 检查是否存在要删除的分区
            check_drop_exists = f"SELECT 1 FROM information_schema.partitions WHERE table_schema = '{dbs}' AND table_name = '{tbs}' AND partition_name = '{date_str_last}'"
            cmd_check_drop_exists = f"mysql -har0607.rwlb.singapore.rds.aliyuncs.com -uives -pCssl#123 -e \"{check_drop_exists}\""
            result_drop_exists = subprocess.run(cmd_check_drop_exists, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

            if "1" in result_drop_exists.stdout.decode('utf-8'):
                # 删除30天前的分区
                sql_drop = f'use {dbs};ALTER TABLE {tbs} DROP PARTITION {date_str_last}'
                cmd_drop = f"mysql -har0607.rwlb.singapore.rds.aliyuncs.com -uives -pCssl#123 -e '{sql_drop}'"
                subprocess.run(cmd_drop, shell=True, check=True)
                print(f"Deleted partition {date_str_last} for table {tbs}")
            else:
                print(f"Partition {date_str_last} does not exist for table {tbs}, skipping deletion")

            # 检查是否存在要添加的分区
            check_add_exists = f"SELECT 1 FROM information_schema.partitions WHERE table_schema = '{dbs}' AND table_name = '{tbs}' AND partition_name = '{date_str_next}'"
            cmd_check_add_exists = f"mysql -har0607.rwlb.singapore.rds.aliyuncs.com -uives -pCssl#123 -e \"{check_add_exists}\""
            result_add_exists = subprocess.run(cmd_check_add_exists, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

            if "1" not in result_add_exists.stdout.decode('utf-8'):
                # 添加七天后的分区
                sql_add = f'use {dbs};ALTER TABLE {tbs} ADD PARTITION (partition {date_str_next} values less than ({next_week_timestamp}))'
                cmd_add = f"mysql -har0607.rwlb.singapore.rds.aliyuncs.com -uives -pCssl#123 -e '{sql_add}'"
                subprocess.run(cmd_add, shell=True, check=True)
                print(f"Added partition {date_str_next} for table {tbs}")
            else:
                print(f"Partition {date_str_next} already exists for table {tbs}, skipping addition")
