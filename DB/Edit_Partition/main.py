#!/usr/bin/python3

# Author        : Ives
# Date          : 2024-10-23


from saas_pro_add_partition2 import DBPartitionManager

def main():

    # telegram 配置
    bot_token = "6327237666:AAEeH1FVThAdnBeYGBkpfWG7HfLy4Jzl_8w"  # 替换为实际的 Bot Token
    chat_id = "-4578699157"      # 替换为实际的 Chat ID
    
    # 分区管理参数
    add_day  = 7       # 添加7天后的分区
    del_day  = 30      # 删除30天前的分区
    edit_num = 8       # 添加或删除分区个数 8等于7个
    interval_days = 1  # 间隔天数

    # 分区表列表
    table_list = [
        'tab_financialelectronic',
        'tab_financialelectronic_jili',
        'tab_financialelectronic_pg',
        'tab_financialelectronic_pp',
        'tab_financialelectronic_spribe',
        'tab_financialelectronic_tb',
        'tab_orderelectronic',
        'tab_orderelectronic_jili',
        'tab_orderelectronic_pg',
        'tab_orderelectronic_pp',
        'tab_orderelectronic_spribe',
        'tab_orderelectronic_tb',
        'tab_ordermakeup',
        'tab_financiallottery_5d',
        'tab_financiallottery_k3',
        'tab_financiallottery_trxwingo',
        'tab_financiallottery_wingo',
        'tab_orderlottery_5d',
        'tab_orderlottery_k3',
        'tab_orderlottery_trxwingo',
        'tab_orderlottery_wingo'
    ]

    # 实例化，并传递公共参数
    notifier = DBPartitionManager(bot_token, chat_id, add_day, del_day, edit_num, interval_days, table_list)





    # 印度1
    topic = f"生产(印度1)--saas系统分区调整情况如上,如无内容则表示无需调整"
    db_host = "pc-gs5ig8zqt3bni2e18.rwlb.singapore.rds.aliyuncs.com"
    db_user = "polar_root"
    db_pwd = "HBho3ePONjjPn9H3CXWi"
    db_list = [

        'tenant_1030',
        'tenant_1017',
        'tenant_1006',
        'tenant_1008',
        'tenant_1027',
        'tenant_1029'
    ]

    notifier.manage_db_partitions(db_host, db_user, db_pwd, db_list, table_list, topic)

    # 南美
    topic = f"生产(南美)--saas系统分区调整情况如上,如无内容则表示无需调整"
    db_host = "pc-gs5s8dc1712fyb97x.rwlb.singapore.rds.aliyuncs.com"
    db_user = "polar_root"
    db_pwd = "sN3DcLa3MDVW3Y2g9WDA"
    db_list = [
        'tenant_1002',
        'tenant_1003',
        'tenant_1019',
        'tenant_1038',
        'tenant_1040'
    ]

    notifier.manage_db_partitions(db_host, db_user, db_pwd, db_list, table_list, topic)

    # 东南亚
    topic = f"生产(东南亚)--saas系统分区调整情况如上,如无内容则表示无需调整"
    db_host = "pc-gs565fd2268h22h9u.rwlb.singapore.rds.aliyuncs.com"
    db_user = "polar_root"
    db_pwd = "ZtagW7NTd3NZYaLFfMbZ"
    db_list = [
        'tenant_1046',
        'tenant_1034',
        'tenant_1043',
        'tenant_1035',
        'tenant_1042',
        'tenant_1052',
        'tenant_1045',
        'tenant_1026',
        'tenant_1044',
        'tenant_1033',
        'tenant_1036',
        'tenant_1039'
    ]

    notifier.manage_db_partitions(db_host, db_user, db_pwd, db_list, table_list, topic)

    # 印度2
    topic = f"生产(印度2)--saas系统分区调整情况如上,如无内容则表示无需调整"
    db_host = "pc-gs59qv41fbw5pg2m8.rwlb.singapore.rds.aliyuncs.com"
    db_user = "polar_root"
    db_pwd = "Yn3z0oQW6mjHWNkbto4j"
    db_list = [
        'tenant_1012',
        'tenant_1025',
        'tenant_1016',
        'tenant_1063',
        'tenant_1064',
        'tenant_1065',
        'tenant_1053',
        'tenant_1047',
        'tenant_1037',
        'tenant_1041',
        'tenant_1021',
        'tenant_1023',
        'tenant_1007'
    ]

    notifier.manage_db_partitions(db_host, db_user, db_pwd, db_list, table_list, topic)

    # 印度3
    topic = f"生产(印度3)--saas系统分区调整情况如上,如无内容则表示无需调整"
    db_host = "pc-gs5waf5yi191sz074.rwlb.singapore.rds.aliyuncs.com"
    db_user = "polar_root"
    db_pwd = "QDGF30HrbXmq3U8vijiq"
    db_list = [
        'tenant_1066',
        'tenant_1011',
        'tenant_1018',
        'tenant_1059',
        'tenant_1001',
        'tenant_1031',
        'tenant_1062',
        'tenant_1014',
        'tenant_1058',
        'tenant_1022'
    ]

    notifier.manage_db_partitions(db_host, db_user, db_pwd, db_list, table_list, topic)

    # 非洲
    topic = f"生产(非洲)--saas系统分区调整情况如上,如无内容则表示无需调整"
    db_host = "pc-gs58ci455v130yx5a.rwlb.singapore.rds.aliyuncs.com"
    db_user = "polar_root"
    db_pwd = "ce8Q3pZi6igaFdqz87mX"
    db_list = [
        'tenant_1004',
        'tenant_1009',
        'tenant_1020'
    ]

    notifier.manage_db_partitions(db_host, db_user, db_pwd, db_list, table_list, topic)

    # 演示站
    topic = f"生产(演示站)--saas系统分区调整情况如上,如无内容则表示无需调整"
    db_host = "pc-gs53gs39aanc9287f.rwlb.singapore.rds.aliyuncs.com"
    db_user = "polar_root"
    db_pwd = "D@2QFL73vXTey3oqsf05K"
    db_list = [
        'tenant_9900',
        'tenant_9901',
        'tenant_9902',
        'tenant_9903',
        'tenant_9904',
        'tenant_9905',
        'tenant_9906',
        'tenant_9907',
        'tenant_9908',
        'tenant_9909',
        'tenant_9910'
    ]

    notifier.manage_db_partitions(db_host, db_user, db_pwd, db_list, table_list, topic)





if __name__ == "__main__":
    main()
