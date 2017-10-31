#!/usr/bin/env python

"""
This module is for Insight DE Code Challenge.

This document implement class "political_donors_finder" and test function test()
"""



import os
import sys



class political_donors_finder(object):
    '''
    This class implements extraction information from raw data
    '''
    
    def __init__(self, outputfile1,outputfile2):
        '''
        Initialize attributes
        fw1 is written to medianvals_by_zip
        fw2 is written to medianvals_by_date

        records_by_zip and records_by_date are dictionaries for storage of zip and date related transactions.
        Key is tuple from CMTE_ID, zip or data.
        Values is list to store transaction information.
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
        Check the validation of the date data
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
        Check the validation of zip data. Currently only checks if it is 5 digits. May check if it is valid zip in USA in future.
        '''
        if len(zipcode) < 5:
            return False
        return True

    def checkamountvaild(self, amt):
        '''
        Check the validation of amount and make sure it is a number.
        '''
        try:
            tmp = int(amt)
        except ValueError:
            return False
        else:
            return True

    def getmedianandsum(self, inp):
        '''
        Get the median, elements. and summation of a list
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

        This funcation is to deal with raw transaction data by external calls.
        First, it divides the raw data mulitple lists, and extracts the information we want
        Second, if validates the extracted data:
            If not individual, or CMTE_ID or amount error, call Return
            If zip is valid, the transaction will be stored into the dictionary records_by_zip, with key containing CMIE_ID and zip. Value is the transaction list returns the real-time median value.
            If the data is valid, the transaction will be stored into the dictionary records_by_date. Value is the transaction list.
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
        This function writes output into files
        Outputs for Medianvals_by_zip is already written to cache and needs flush
        Outputs for Medianvals_by_data is both generated and written here 

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
        Provides some running information
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

    


    for line in fp:
        poli_finder.processline(line)

    
    poli_finder.flush()    

    poli_finder.status()
        
        



if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Parameter error, please check!")
    else:
        test(sys.argv[1],sys.argv[2],sys.argv[3])
