# coding:utf-8

'''
 去除重复行
'''

if __name__ == '__main__':
    readDir = "E:/1.txt"
    writeDir = "E:/2.txt"
    outfile = open(writeDir, "w")
    f = open(readDir, "r")

    lines_seen = set()  # Build an unordered collection of unique elements.

    for line in f:
        line = line.strip('\n').strip().upper()
        if 'SDL' in line:
            continue;
        if 'MDL' in line:
            continue;
        if 'OPERAT' in line:
            continue;
        if 'LAB' in line:
            continue;
        if 'DW_KPI' in line:
            continue;
        if line not in lines_seen:
            outfile.write(line + '\n')
            lines_seen.add(line)
    print("==============")