# -*-coding:utf-8-*-

import pyodbc

# 連線資料庫（不需要配置資料來源）,connect()函式建立並返回一個 Connection 物件
cnxn = pyodbc.connect(r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=.\flowControl.mdb')
# cursor()使用該連線建立（並返回）一個遊標或類遊標的物件
crsr = cnxn.cursor()

# 列印資料庫LOG.mdb中的所有表的表名
print('`````````````` LOG-Tables ``````````````')
for table_info in crsr.tables(tableType='TABLE'):
    print(table_info.table_name)

# 取得 LOG_rasterIO 資料表內容
print('`````````````` Select Command ``````````````')
crsr.execute("SELECT ZipFileName,Progress from FlowCtrl WHERE Progress LIKE '%99:%' ORDER BY Priority")   # 此時 rows 即 cursor
rows = crsr.fetchall()
print(len(rows))
#for item in rows:
#    print(item[0]+','+item[1])
print(rows[0][0]+','+rows[0][1])

# 新增測試
#print('`````````````` insert Command ``````````````')
#crsr.execute("INSERT INTO LOG_rasterIO VALUES('11112222','',0,0,0,'','','','',16)")
#print(crsr.rowcount)

# update測試
#print('`````````````` update Command ``````````````')
#crsr.execute("UPDATE LOG_rasterIO SET Status=2,ErrMsg='123abc' ")
#print(crsr.rowcount)


# 提交資料（只有提交之後，所有的操作才會對實際的物理表格產生影響）
crsr.commit()
crsr.close()
cnxn.close()

