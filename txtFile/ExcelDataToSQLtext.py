import xlrd

'''
环境准备：  pip install xlrd==1.2.0 -i  https://pypi.tuna.tsinghua.edu.cn/simple

读取excel中列英文名、中文名、字段类型，拼接成sql语句。
如：
    .....
    creat_time string COMMENT '创建时间' ,
    update_time string COMMENT '更新时间' ,
    ......
'''
class ExcelData():
    # 初始化方法
    def __init__(self, data_path, sheetname):
        #定义一个属性接收文件路径
        self.data_path = data_path
        # 定义一个属性接收工作表名称
        self.sheetname = sheetname
        # 使用xlrd模块打开excel表读取数据
        self.data = xlrd.open_workbook(self.data_path)
        # 根据工作表的名称获取工作表中的内容（方式①）
        self.table = self.data.sheet_by_name(self.sheetname)

    '''
    rows_no_start: 数据起始行
    rows_no_end: 数据结束行
    col:   英文名称在第几列
    col_cn: 中文名称在第几列
    col_type: 字段类型在第几列
    '''
    def excelToSQL(self,rows_no_start,rows_no_end,col,col_cn,col_type):
        # 定义一个空列表
        datas = []
        j = rows_no_start
        while j < rows_no_end:
            #print("j :" + str(j))
            column = self.table.cell_value(j, col)
            column_cn = self.table.cell_value(j, col_cn)
            column_type = self.table.cell_value(j, col_type)
            content = "{col_txt} {col_type_txt} COMMENT '{col_cn_txt}' ,".format(col_txt=column, col_type_txt=column_type, col_cn_txt=column_cn)
            #print(content)
            datas.append(content)
            j = j+1
        return datas

    def excelToSQL2(self,rows_no_start,rows_no_end,col):
        # 定义一个空列表
        datas = []
        j = rows_no_start
        while j < rows_no_end:
            #print("j :" + str(j))
            column = self.table.cell_value(j, col)
            content = "{col_txt},".format(col_txt=column)
            print(content)
            datas.append(content)
            j = j+1
        return datas
    '''
        数组写入txt文件
    '''
    def writeToTxt(self, file_path, data_array):
        with open(file_path, "a") as file:
            for i in data_array:
                file.write(i + "\n")

if __name__ == "__main__":
    data_path = "D:/gitDoc/ts-fdw-doc\数仓设计和开发/1.SRC&ODS层/沃分期系统-ISIM/THIRD_ORDER_INFO-沃分期3.0订单表.xlsx"
    sheetname = "THIRD_ORDER_INFO"
    get_data = ExcelData(data_path, sheetname)
    #datas = get_data.excelToSQL(8, 83, 17, 18, 19)
    datas = get_data.excelToSQL2(8, 83, 9)
    #print(datas)
    txt_name = "E:/test2.txt"
    get_data.writeToTxt(txt_name, datas)