#!/usr/bin/env python
# encoding: utf-8

from bs4 import BeautifulSoup
import os
import sqlite3
import traceback
import sys

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

def singleton(class_):
    instances = {}
    def getinstance(*args, **kwargs):
        if class_ not in instances:
            instances[class_] = class_(*args, **kwargs)
        return instances[class_]
    return getinstance

def list2str(l):
    result = '[' + ','.join(l) + ']'
    return result

@singleton
class DbHelper():
    def __init__(self):
        self.conn = sqlite3.connect("code.db")
        self.createTable()
        self.buf = []

    def createTable(self):
        self.conn.execute('''
CREATE TABLE IF NOT EXISTS xzcode (
    id INTEGER AUTO_INCREMENT,
    code VARCHAR(20),
    province VARCHAR(50),
    city VARCHAR(50),
    county VARCHAR(50),
    town VARCHAR(50),
    village VARCHAR(50),
    extraCode VARCHAR(50),
    PRIMARY KEY (id))''')
        self.conn.commit()

    def saveCode(self, code, province = "", city = "", county = "", town = "", village = "", extraCode = ""):
        self.buf.append([code, province, city, county, town, village, extraCode])
        if (len(self.buf) >= 10000):
            self.flushBuf()

    def flushBuf(self):
        print "flush buffer to db"
        for item in self.buf:
            # delSql = '''DELETE FROM xzcode WHERE code = '%s';'''%(item[0])
            # # print delSql
            # self.conn.execute(delSql)
            
            insertSql = '''
INSERT INTO xzcode (code, province, city, county, town, village, extraCode)
VALUES('{}', '{}', '{}', '{}', '{}', '{}', '{}');'''.format(item[0], item[1], item[2], item[3], item[4], item[5], item[6])
            # print insertSql
            self.conn.execute(insertSql)
        print "commit change and reset buffer"
        self.conn.commit()
        del self.buf[:]

class Extractor():
    def extract(self, paths, level):
        if level == "province":
            cityPaths = self.extractCity(paths)
            countyPaths = self.extractCounty(cityPaths)
            townPaths = self.extractTown(countyPaths)
            self.extractVillage(townPaths)
        elif level == "city":
            countyPaths = self.extractCounty(paths)
            townPaths = self.extractTown(countyPaths)
            self.extractVillage(townPaths)
        elif level == "county":
            townPaths = self.extractTown(countyPaths)
            self.extractVillage(townPaths)
        elif level == "town":
            self.extractVillage(paths)

    def extractCity(self, provincePaths):
        return self.extractCol(provincePaths, "city")

    def extractCounty(self, cityPaths):
        return self.extractCol(cityPaths, "county")

    def extractTown(self, countyPaths):
        return self.extractCol(countyPaths, "town")

    def extractVillage(self, townPaths):
        return self.extractCol(townPaths, "village")

    def extractCol(self, paths, level):
        className = level + "tr"
        nextPaths = []
        for path, context in paths:
            try:
                with open(path, "r") as doc:
                    bs = BeautifulSoup(doc.read().decode("gb18030", "replace"))
                allTrs = bs.find_all("tr", class_ = className)
                
                # missing level, maybe the level is just missing, we continue to search the next level
                if not allTrs:
                    print "MISSING LEVEL: continue search next level when searching {} in {}".format(path, list2str(context))
                    newContext = context[:]
                    newContext.append("")
                    nextPaths.append((path, newContext))
                    continue
                 
                for tr in allTrs:
                    tds = tr.find_all("td")

                    if level == "village":
                        code = tds[0].string
                        extraCode = tds[1].string
                        name = tds[2].string.encode("utf8")
                        print "YES: {} in {}: {}, {}, {}".format(path, list2str(context), code, extraCode, name)
                        DbHelper().saveCode(code, province = context[0], city = context[1],\
                            county = context[2], town = context[3], village = name, extraCode = extraCode)
                        continue

                    # not village
                    codeLink = tds[0].find("a")
                    code = ""
                    name = ""
                    href = ""
                    if not codeLink or len(codeLink) == 0:
                        code = tds[0].string
                        name = tds[1].string.encode("utf8")
                    else:
                        href = codeLink.attrs["href"]
                        code = codeLink.string
                        name = tds[1].find('a').string.encode("utf8")
                    print "YES: {} in {}: {}, {}, {}".format(path, list2str(context), href, code, name)
                    if href:
                        newContext = context[:]
                        newContext.append(name)
                        nextPaths.append((self.convertPath(path, href), newContext))
                    else:
                        "NO href: when extractCol {} in {}".format(path, list2str(context))

                    if level == "city":
                        DbHelper().saveCode(code, province = context[0], city = name)
                    elif level == "county":
                        DbHelper().saveCode(code, province = context[0], city = context[1],\
                            county = name)
                    elif level == "town":
                        DbHelper().saveCode(code, province = context[0], city = context[1],\
                            county = context[2], town = name)
            except Exception, e:
                print "WTF: error when extractCol {} in {}".format(path, list2str(context))
                traceback.print_exc(file=sys.stdout)
        return nextPaths

    def convertPath(self, current, relative):
        return os.path.join(os.path.dirname(current), relative)

if __name__ == "__main__":
    extractor = Extractor()
    for code in xrange(11, 66):
        codeStr = str(code)
        if codeStr not in code2province:
            continue
        print "\n======== ", code2province[codeStr].encode('utf8'), " =========="
        provincePaths = [(os.path.join("./2013", codeStr + ".html"), [code2province[codeStr].encode('utf8')])]
        extractor.extract(provincePaths, "province")
    DbHelper().flushBuf()