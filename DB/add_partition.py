# /usr/bin/python3

# Author        : Daniel
# Date          : 2023-05-30
import datetime
import subprocess

# 每周一凌晨12点定时执行即可

tables_list = [
    'tbl_game_record',
    'tbl_game_record_qp',
    'tbl_game_record_ty',
    'tbl_game_record_zr',
    'tbl_game_record_fc',
    'tbl_game_record_dz',
    'tbl_game_record_es',
    'tbl_game_record_mini',
    'tbl_game_record_fish',
    'tbl_balance_transaction'
]

# 获取当前日期和时间
current_date = datetime.datetime.now()

# 计算下一个周一的日期
days_ahead = (0 - current_date.weekday() + 1) % 7
next_monday = current_date + datetime.timedelta(days=days_ahead)

# 设置每周一凌晨1点的时间
target_time = datetime.time(1, 0, 0)

# 组合日期和时间
target_datetime = datetime.datetime.combine(next_monday.date(), target_time)

# 下周时间戳
next_week = target_datetime + datetime.timedelta(weeks=1)
next_week_timestamp = int(next_week.timestamp() * 1000)

# 获取下一个周一的年、月、日
year_str = str(next_monday.year)
month_str = str(next_monday.month).zfill(2)
day_str = str(next_monday.day).zfill(2)

date_str = "p" + year_str + month_str + day_str

for tbs in tables_list:
    # 执行添加表分区的操作
    sql = f'use test_p3; ALTER TABLE {tbs} ADD PARTITION {date_str} values less than ("{next_week_timestamp}");'
    # print(sql)
    print(next_week)
    print(next_week_timestamp)
    # cmd = f"mysql -h10.170.0.26 -uroot -P9030 -e '{sql}'"
    # subprocess.run(cmd, shell=True, check=True)
