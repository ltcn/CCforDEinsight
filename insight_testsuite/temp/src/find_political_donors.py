#!/usr/bin/env python

"""
This module is for Insight DE Code Challenge.
本文档实现类 political_donors_finder 和一个测试函数test()
"""



import os
import sys



class political_donors_finder(object):
    '''
    这个类实现从原始数据中提取有趣信息的工作
    '''
    
    def __init__(self, outputfile1,outputfile2):
        '''
        初始化将要用的变量
        fw1 用于写出文件medianvals_by_zip
        fw2 用于写出文件medianvals_by_date

        records_by_zip
        records_by_date
        类型是字典，分别用于存储 zip和date相关的交易数据。key是CMTE_ID和zip
        或者date构成的元组，values是list，存储交易信息。
        '''

        self.fw1 = outputfile1
        self.fw2 = outputfile2

        #count processed line
        self.processedlinecount = 0
        # ignorecont for zip, date, other_id invaild
        self.ignorecount = [ 0 , 0 , 0 ] 
    
        self.records_by_zip = {}
        self.records_by_date = {}



    def checkdatevaild(self, s_date):
        '''
        检查日期合法性
        '''
        if len(s_date)!=8:
            return False
        try:
            mm = int(s_date[0:2])
            dd = int(s_date[2:4])
            yyyy = int(s_date[4:8])
        except ValueError:
            return False

        daysforFeb = 29
        if (yyyy %100 != 0 and yyyy %4==0) or yyyy%400 ==0 :
            daysforFeb = 30
            
        
        if mm in [1,3,5,7,8,10,12] and 0<dd<32:
            return True
        elif mm in [ 4,6,9,11] and 0 < dd<31:
            return True
        elif mm == 2 and 0<dd<daysforFeb:
            return True
        else:
            return False

    def checkzipvaild(self, zipcode):
        '''
        检查zip合法性，目前仅检查是否是五位长度。可以增加检测是否是合法美国zip
        '''
        if len(zipcode) < 5:
            return False
        return True

    def checkamountvaild(self, amt):
        '''
        检查amount的合法性，确认是一个数字
        '''
        try:
            tmp = int(amt)
        except ValueError:
            return False
        else:
            return True

    def getmedianandsum(self, inp):
        '''
        获取一个数列的中位数，所含元素数目，和总和
        '''
        inp.sort()
        n = len(inp)
        if n == 0:
            return (0,0,0)
        med = 0
        if n%2 == 0:
            med = (inp[n//2] + inp[n//2-1])/2
            med = int(med) if med-int(med) < 0.5 else int(med) + 1
        else:
            med = inp[n//2]

        return (med,n,sum(inp))

    def processline(self, line):
        '''
        :type line: str
        :rtype: void

        此函数用于外部调用对输入的交易原始数据进行处理。
        首先，原始交易数据会被分割为数列，仅从中提取我们关心的部分
        然后我们检测该行数据的有效性：
            如果不属于individe的，或者CMTE_ID和amount无效的，直接返回
            
            如果包含了有效的zip信息，将该交易记录到以CMTE_ID和zip字符串为
             key的字典records_by_zip中，value是所有属于该ID和zip的交易list，
             同时输出实时中位数信息
             
            如果包含了有效的date信息，将该交易记录到以CMTE_ID和zip字符串为
             key的字典records_by_date中，value是所有属于该ID和date的交易list
        '''

        self.processedlinecount += 1
         
        line = line.split("|")
        line = [ y for x,y in enumerate(line) if x ==0 or x == 10 or x == 13 or x==14 or x==15]

        """
        :line[0]: id
        :line[1]: zip
        :line[2]: date
        :line[3]: amount
        :line[4]: other id, must be empty
        """


        if len(line[1]) > 5:
                line[1] = line[1][:5]
        if line[4] != '' or line[0]=="" or self.checkamountvaild(line[3]) == False:
            self.ignorecount[2]+=1
            return

        CMTE_ID = line[0]
        ZIPCODE = line[1]
        T_DATE = line[2]
        T_AMT = int(line[3])

        
        if self.checkzipvaild(ZIPCODE):  #zip is vaild, add into record
            index = (CMTE_ID,ZIPCODE)
            if self.records_by_zip.get(index)==None:
                self.records_by_zip[index] = [T_AMT]
            else:
                self.records_by_zip[index].append(T_AMT)
            med, n,total = self.getmedianandsum(self.records_by_zip[index])
            self.fw1.write("{}|{}|{}|{}|{}".format(index[0],index[1],med,n,total) + os.linesep)
        else:
            self.ignorecount[0]+=1
        
        if self.checkdatevaild(T_DATE):  #DATE is vaild, add into record
            index = (CMTE_ID,T_DATE)
            if self.records_by_date.get(index)==None:
                self.records_by_date[index] = [T_AMT]
            else:
                self.records_by_date[index].append(T_AMT)
        else:
            self.ignorecount[1]+=1


    def flush(self):
        '''
        此函数用于将内容写入到文件中
        medianvals_by_zip的内容在每次处理时已经写入缓存，仅需要flush
        medianvals_by_date的内容会在这里生产并一次性写入
        '''

        self.fw1.flush()
        
        datekeys = list(self.records_by_date.keys())
        datekeys.sort()
        self.fw2.seek(0,0)
        for i in datekeys:
            med, n,total = self.getmedianandsum(self.records_by_date[i])
            self.fw2.write("{}|{}|{}|{}|{}".format(i[0],i[1],med,n,total) + os.linesep)
        self.fw2.flush()

    def status(self):
        '''
        提供一些运行历史信息
        '''
        print("\nTotal processed {} lines".format(self.processedlinecount))
        print("\ningore {} lines because zip is not vaild".format(self.ignorecount[0]))
        print("\ningore {} lines because date is not vaild".format(self.ignorecount[1]))
        print("\ningore {} lines because ID or others malformed".format(self.ignorecount[2]))


def test(inputfile,outputfile1,outputfile2):
    '''
    INPUTFILE = os.path.dirname(os.getcwd()) + os.sep + "input" + os.sep + "itcont.txt"

    OUTPUTFILE1 = os.path.dirname(os.getcwd()) + os.sep + "output" + os.sep + \
               "medianvals_by_zip.txt"
    OUTPUTFILE2 = os.path.dirname(os.getcwd()) + os.sep + "output" + os.sep + \
               "medianvals_by_date.txt"
    '''
    try:
        fp = open(inputfile,'r')
        fw1 = open(outputfile1,'w')
        fw2 = open(outputfile2,'w')
    except IOError as e:
        print("I/O error ({}):{}".format(e.errno,e.strerror))
        return

    poli_finder =  political_donors_finder(fw1,fw2)

    
    count = 0


    for line in fp:
        poli_finder.processline(line)
#        count+=1
        if count > 100000:
            break

    
    poli_finder.flush()    

    poli_finder.status()
        
        



if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Parameter error, please check!")
    else:
        test(sys.argv[1],sys.argv[2],sys.argv[3])
