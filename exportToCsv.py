#!/usr/bin/env python
# encoding: utf-8

import csv
import os
import sqlite3

code2province = {
    "11": u"北京市",
    "12": u"天津市",
    "13": u"河北省",
    "14": u"山西省",
    "15": u"内蒙古自治区",
    "21": u"辽宁省",
    "22": u"吉林省",
    "23": u"黑龙江省",
    "31": u"上海市",
    "32": u"江苏省",
    "33": u"浙江省",
    "34": u"安徽省",
    "35": u"福建省",
    "36": u"江西省",
    "37": u"山东省",
    "41": u"河南省",
    "42": u"湖北省",
    "43": u"湖南省",
    "44": u"广东省",
    "45": u"广西壮族自治区",
    "46": u"海南省",
    "50": u"重庆市",
    "51": u"四川省",
    "52": u"贵州省",
    "53": u"云南省",
    "54": u"西藏自治区",
    "61": u"陕西省",
    "62": u"甘肃省",
    "63": u"青海省",
    "64": u"宁夏回族自治区",
    "65": u"新疆维吾尔自治区"
}

def createCsv(filename):
    # file for whole china
    file = open(filename, "wb")
    # BOM for utf8
    file.write("\xEF\xBB\xBF")
    writer = csv.writer(file, dialect=csv.excel)
    return (file, writer)

if __name__ == '__main__':
    # make the csv path for store csv files
    csvPath = "./csv"
    if not os.path.exists(csvPath):
        os.makedirs(csvPath)
    
    # sqlite3 connection
    conn = sqlite3.connect('code.db')
    cursor = conn.execute("SELECT code, province, city, county, town, village, extracode FROM xzcode ORDER BY code")
    
    # header
    header = [item.encode("utf8") for item in (u"代码",u"省",u"市",u"县",u"镇",u"村",u"城乡分类")]
    
    # save writer for each province
    files = {}
    writer = {}
    
    # file for whole china
    totalFile, totalWriter = createCsv(os.path.join(csvPath, u"全国.csv"))
    totalWriter.writerow(header)
    
    # iterate the data and generate csv
    for row in cursor:
        utf8Row = [item.encode("utf8") for item in row]
        # china
        totalWriter.writerow(utf8Row)
        
        # each province
        provinceCode = row[0][:2]
        province = code2province[provinceCode]
        if provinceCode not in files:
            files[provinceCode], writer[provinceCode] = createCsv(os.path.join(csvPath, province + ".csv"))
            writer[provinceCode].writerow(header)
        writer[provinceCode].writerow(utf8Row)

    for code in files:
        files[code].close()
    totalFile.close()
    
    # close the sqlite3 connection
    conn.close()