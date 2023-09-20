import sqlite3

conn = sqlite3.connect('./data.sqlite3')

# 获取游标
cur = conn.cursor()

# 执行SQL
cur.execute("select * from data")

# 获取查询结果
print(cur.fetchall())  # 取出所有（返回列表套元组或空列表）

# 记得关闭游标和连接，避免内存泄露
cur.close()
conn.close()
