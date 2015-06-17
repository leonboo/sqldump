#coding: utf-8
import MySQLdb
import types

class DumpData():
    destFile = ''
    dataEncode = 'utf8'
    head = ''
    fetchNum = 1000

    host = ''
    db = ''
    port = 0
    user = ''
    pwd = ''
    charset = ''
    table = ''

    def __init__(self, savePath):
        self.destFile = savePath
        if os.path.exists(savePath):
            os.remove(savePath)

    def dumpData(self, *args):
        pass

    def saveData(self,data):
        if not data:
            return
        with open(self.destFile, 'a') as wf:
            wf.write(data)

    def constructData(self):
        pass

    def formatStr(self, src):
        res = []
        for x in src:
            if not x:
                res.append('')
            elif type(x) is types.UnicodeType:
                res.append(x.encode(self.dataEncode).replace('\n', ''))
            else:
                res.append(str(x))
        return res
        

    def logExcept(self, err):
        pass

import json
import os
class MySQLData(DumpData):
    def writeHead(self):
        self.saveData(self.head+'\n')

    def dumpData(self, host=None, port=None, user=None, pwd=None, db=None, table=None, head=None, findRex=None, charset=None):
        try:
            self.host = host
            self.port = port
            self.user = user
            self.db = db
            self.pwd = pwd 
            self.table = table
            self.charset = charset
            self.head = head
            self.findRex = findRex
            self.writeHead()
            sql = 'select %s from %s ' %(self.head, self.table)
            if self.findRex:
                sql += 'where %s' % self.findRex
            shop_con = MySQLdb.connect(host = self.host, user = self.user, passwd = self.pwd, db = self.db, port = self.port, charset=self.charset)
            cur = shop_con.cursor(MySQLdb.cursors.SSCursor)                   
            cur.execute(sql)   
            cnt = 0;
            with open(self.destFile, 'a') as wf:
                while 1:
                    rows = cur.fetchmany(self.fetchNum) 
                    if not rows:
                        break
                    cnt += len(rows)
                    res = self.constructData(rows) 
                    wf.write(res)
            cur.close()
            return cnt
        except Exception,e:
            self.logExcept(e)
            return 0
       
    def constructData(self, rows):
        res = ''
        for row in rows:
            try:
                tmp = self.formatStr(row)
                if tmp:
                    res += ','.join(tmp) + '\n'
            except Exception,e:
                self.logExcept(e)
        return res

import pymongo
class MongoData(DumpData):
    write_per_cnt = 2000
    def writeHead(self):
        self.saveData(self.head + '\n')

    def dumpData(self, host=None, port=None, user=None, pwd=None,db=None, table=None,head=None,findRex=None):
        try:
            self.host = host
            self.port = port
            self.user = user
            self.pwd = pwd
            self.table = table
            self.head = head
            self.db = db
            self.findRex = findRex
            self.writeHead()
            con = pymongo.Connection(host = self.host, port = self.port)
            admin = con.admin
            admin.authenticate(user, pwd)
            dbs = con[self.db][self.table]
            res = ''
            cnt = 1
            for row in dbs.find(self.findRex):
                tmp = self.getDataStr(row)
                if tmp:
                    res += ','.join(tmp) + '\n'
                if res and cnt % self.write_per_cnt == 0:
                    self.saveData(res)
                    res = ''
                cnt += 1
            if res:
                self.saveData(res)
            con.close()
        except Exception,e:
            self.logExcept(e)
        
    def getDegree(self, degree):
        pass

    def getDataStr(self, row):
        headlist = self.head.split(',')
        data = []
        for h in headlist:
            value = row.get(h, '')
            data.append(value)
        return self.formatStr(data)

class ImageData(MongoData):
    degreeDict = {'0':'ORIG','1':'ID_52X52','2':'ID_75X75','3':'ID_81X71','4':'ID_122X108','5':'ID_240X320','6':'ID_320X240','7':'ID_320X480','8':'ID_480X800','9':'ID_150X150','10':'ID_162X142'}
    def writeHead(self):
        res = self.head.split(',')
        res.remove('degree')
        for key,value in self.degreeDict.items():
            degreeHead = 'degree_' + str(value)
            res.append(degreeHead)
        self.saveData(','.join(res) + '\n')

    def getDegree(self, degree):
        res = []
        src = {}
        if degree:
            src = degree
        for key,value in self.degreeDict.items():
            imageAddr = src.get(key, '')
            res.append(imageAddr)
        return res
    def getDataStr(self, row):
        headlist = self.head.split(',')
        data = []
        for h in headlist:
            value = row.get(h, '')
            if h == 'degree':
                data.extend(self.getDegree(value))
            else:
                data.append(value)
        return self.formatStr(data)

class ShopData(MySQLData):
    def writeHead(self):
        headL = []
        for x in self.head.split(','):
            if x == 'attributes':
                headL.append('charge_type')
            else:
                headL.append(x)
        self.saveData(','.join(headL) + '\n')

    def __get_charge_type(self, attributes):
        charge_type = '0'
        try:
            attr = json.loads(attributes)
            charge_type = attr.get('charge_type', '0')
            if not charge_type:
                charge_type = '0'
        except Exception,e:
            self.logExcept(e)
        return charge_type

    def constructData(self, rows):
        res = ''
        for row in rows:
            try:
                charge_type = self.__get_charge_type(str(row[13].encode(self.dataEncode)))
                rowlist = list(x for x in row)
                rowlist[13] = charge_type
                tmp = self.formatStr(rowlist)
                if tmp:
                    res += ','.join(tmp) + '\n'
            except Exception,e:
                self.logExcept(e)
        return res

def getYestodayCnt(configFile):
    res = 0
    if(os.path.exists(configFile)):
        with open(configFile, 'r') as rf:
            s = rf.readline()
            try:
                res = int(s)
            except Exception,e:
                pass
    return res


def Test():
    host = '172.16.18.203'
    port = 3306
    user = 'data'
    pwd = 'opensesame'
    db = 'acdb'
    charset = 'utf8'
    head = 'id,name,address,telno,trade_id,x,y,province_id,city_id,district_id,source,introduction,landmark,attributes'
    table = 'basepoi'
    #import pdb
    #pdb.set_trace()

    shop = 'shop.csv'
    shop = ShopData(shop)
    findRex = 'is_disp=1 limit 100000;'
    shop.dumpData(host, port, user, pwd, db, table, head, findRex, charset)

if __name__ == '__main__':
    Test()
