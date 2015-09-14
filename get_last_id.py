#coding=utf-8
from configparser import ConfigParser
import requests
import logging
import sys
import time
import shutil
import os

#{"id":105574163,"caseCode":"(2015)丰执字第01477号","caseState":"执行中","execCourtName":"徐州市丰县人民法院","execMoney":377352.1,"partyCardNum":"32032119860****3830","pname":"谷勇","caseCreateTime":"2015年07月08日"}

                
def main():
    store_file = 'result.txt'
    if os.path.exists(store_file):
        with open(store_file, "r") as f:
            try:
                f.seek(-512, 2)
                lines = f.readlines()
            except:
                f.seek(0, 0)
                lines = f.readlines()
            finally:
                last_id = lines[-1].split('\t')[-1].split(':')[-1]
                start_id = int(last_id) + 1
                print(start_id)

if __name__ == '__main__':
    main()
