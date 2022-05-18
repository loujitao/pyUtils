# -*- coding: utf-8 -*-
import os



#设定文件路径
path = 'C:\\Users\\unicom\\Documents\\WXWork\\1688850730015035\\Cache\\File\\2022-05\\脚本梳理\\脚本梳理\\ods'
#对目录下的文件夹进行遍历
for file in os.listdir(path):

    #判断是否是文件
    if os.path.isdir(os.path.join(path,file))==True:
        newname = 'ods_'+file.replace('-', '').lower()
        print(newname)
        oldname = os.path.join(path, file)
        os.rename(oldname, os.path.join(path, newname))
#结束
print ("End")
