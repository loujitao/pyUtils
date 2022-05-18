# -*- coding: utf-8 -*-
import os



#设定文件路径
path = 'D:\\zjl\\'
#对目录下的文件进行遍历
for file in os.listdir(path):

    #判断是否是文件
    if os.path.isfile(os.path.join(path,file))==True:

        newname = file
        if '05.' in file:
            newname=newname.replace('05.', ' ')
        if '周杰伦' in file:
            newname=newname.replace('周杰伦', '')
        if '_' in file:
            newname=newname.replace('_', '')
        if '+' in file:
            newname=newname.replace('+', '')
        if '-' in file:
            newname=newname.replace('-', '')
        else:
            pass
        oldname = os.path.join(path, file)
        os.rename(oldname, os.path.join(path, newname))
#结束
print ("End")
