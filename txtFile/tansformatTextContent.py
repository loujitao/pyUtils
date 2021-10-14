import re
'''
    "......
    TM_STMP	STRING	COMMENT '时间戳',
    ......"
格式的文件，拼接成：
    "TM_STMP,T_TMP,TP,TMP"
'''


filePath = 'D:\\testData\\python\\tsvData.txt'

columnArr = []

with open(filePath, 'r') as tsv:
    contents = tsv.readlines()       #读取全部行
    for content in contents:       #显示一行
        # print(content.split('\t')[0])   #每行用逗号分隔后，取第一个元素
        col = content.split('\t')[0]

        if re.search("\s",col):     #单列去空格
            col = col.split()[0]
        columnArr.append( col )
    # print(columnArr)

f = "D:\\testData\\python\\tsvData_2.txt"
with open(f,"a") as file:
    for i in columnArr:
        file.write( i + ","+"\n")