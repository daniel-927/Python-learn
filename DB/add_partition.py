# /usr/bin/python3

# Author        : Daniel
# Date          : 2023-05-30
import datetime
import subprocess


# 每天定时执行即可

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

# 获取当前日期和时间
current_date = datetime.datetime.now()

# 计算七天后的日期
next_week = current_date + datetime.timedelta(days=7)

# 计算30天前的日期
last_30_days = current_date - datetime.timedelta(days=30)

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

for tbs in tables_list:
    # 删除30天前的分区
    sql_drop = f'use tenant_1001;ALTER TABLE {tbs} DROP PARTITION {date_str_last}'
    cmd_drop = f"mysql -har0607.rwlb.singapore.rds.aliyuncs.com -uives -pCssl#123 -e '{sql_drop}'"
    subprocess.run(cmd_drop, shell=True, check=True)

    # 添加七天后的分区
    sql_add = f'use tenant_1001;ALTER TABLE {tbs} ADD PARTITION (partition {date_str_next} values less than ({next_week_timestamp}))'
    cmd_add = f"mysql -har0607.rwlb.singapore.rds.aliyuncs.com -uives -pCssl#123 -e '{sql_add}'"
    subprocess.run(cmd_add, shell=True, check=True)

    print(f"Updated partitions for table {tbs}: Added {date_str_next}, Removed {date_str_last}")

