import os
'''
    文件名批量修改工具
'''

#设定文件路径
path='E:\\大数据文档\\'
i=1
#对目录下的文件进行遍历
for file in os.listdir(path):

    #判断是否是文件
    if os.path.isfile(os.path.join(path,file))==True:
        oldname = os.path.join(path, file)

        if file.startswith('[www.java1234.com]') == True:
            newname=file.replace('[www.java1234.com]', '')
            os.rename(oldname, os.path.join(path,newname) )

        elif file.endswith('@www.java1234.com.pdf') == True:
            newname=file.replace('@www.java1234.com.pdf', 'B.pdf')
            os.rename(oldname, os.path.join(path, newname))

        elif file.endswith('www.java1234.com.docx') == True:
            newname=file.replace('www.java1234.com.docx', '.docx')
            os.rename(oldname, os.path.join(path, newname))

        elif '尚硅谷大数据技术之' in file:
            newname=file.replace('尚硅谷大数据技术之', '')
            os.rename(oldname, os.path.join(path, newname))
        else:
            pass
#结束
print ("End")