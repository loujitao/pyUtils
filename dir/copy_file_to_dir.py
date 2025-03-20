import shutil
import os

'''
功能：将单文件，复制到文件夹下所有子目录中，各一份
参数：file_path  待拷贝源文件
      dir   文件输出到的文件夹
'''
def copy_to_batch_dir(file_path, root_dir):
    if os.path.isfile(root_dir):
        pass
    if os.path.isdir(source_dir):
        for x in os.listdir(root_dir):
            file_dir = root_dir + '/' + x
            if os.path.isdir(file_dir):
                print("正在复制到" + file_dir)
                shutil.copy(file_path, file_dir)
                #如果子文件夹还需要拷贝，解开下列注释，递归调用
                #copy_to_batch_dir(file_path, file_dir)
#####cc

'''
功能：将单个文件，复制到单个目录下， 一份
参数：file_path  待拷贝源文件
      dir   文件输出到的文件夹
'''
def single_copy(file_path, dir):
    if os.path.isdir(source_dir):
        print("源不是文件！是文件夹")
        return
    shutil.copy(file_path, dir)

'''
功能：将目录下所有文件，复制到单个目录下，一份
参数：source_dir  待拷贝源文件夹
      dir   文件输出到的文件夹
'''
def batch_file_copy(source_dir, dir):
    if os.path.isfile(source_dir):
        print("源不是文件夹！")
        return
    all_list = os.listdir(source_dir)
    print("文件个数：" + str( len(all_list) ))
    for file in all_list:
        #print("文件名："+file)
        source_file = source_dir + "/" + file
        shutil.copy(source_file, dir)

'''
功能： 递归地将目录及其子文件夹所有文件，复制到某一个目录下
参数：source_dir  待拷贝源文件夹
      dir   文件输出到的文件夹
'''
def batchfile_to_dir(source_dir, dir):
    #如果是文件
    if os.path.isfile(source_dir):
        #筛选需要的文件拷贝
        if source_dir.endswith("epub"):
            shutil.copy(source_dir, dir)
        else:
            print("不是epub文件，名字："+source_dir)
    #如果是文件夹
    if os.path.isdir(source_dir):
        for s in os.listdir(source_dir):
            # 子文件夹继续递归判断
            newdir = source_dir + "/" + s
            batchfile_to_dir(newdir, dir)

if __name__ == '__main__':
    #source_file = r'E:/测试/1.jpg'
    #source_dir = r'E:\excel'
    #output_dir = r'D:\gitDoc\ts-fdw-doc\数仓设计和开发\1.SRC&ODS层\统一认证授权系统(WOAUTH)'

    #single_copy(source_file,output_dir)
    #batch_file_copy(source_dir, output_dir)


    source_dir = r'C:\Users\unicom\AppData\Roaming\NeatReader\bookData'
    output_dir = r'E:\aaaa'
    batchfile_to_dir(source_dir, output_dir)


