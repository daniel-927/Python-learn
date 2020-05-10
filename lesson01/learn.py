# learn-day01
# 交叉赋值
# x, y, z = 3, 4, 5
# z, y, x = x, y, z
# print(x, y, z)
#
# score = input("please enter:")
# score = int(score)
#
# if score >= 90:
#     print("优秀")
# elif score >= 80:
#     print("良好")
# elif score >= 70:
#     print("一般")
# else:
#     print("all is sb")

age = 22
is_beautiful=True
star = '天平座'

if 16 < age < 20 and is_beautiful and star == '天平座':
    print("开始表白....")
    is_beautiful=True
    if is_beautiful:
        print('两个人从此过上了没羞没臊的生活。')
else:
    print('阿姨好，打扰了！')