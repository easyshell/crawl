#coding=utf-8
import sys
if "/home/kimi/kimi/division/module/futures-2.1.6" not in sys.path:
    sys.path.append("/home/kimi/kimi/division/module/futures-2.1.6")
from concurrent.futures import ThreadPoolExecutor
from configparser import ConfigParser
import requests
import logging
import time
import shutil
import os

#{"id":105574163,"caseCode":"(2015)丰执字第01477号","caseState":"执行中","execCourtName":"徐州市丰县人民法院","execMoney":377352.1,"partyCardNum":"32032119860****3830","pname":"谷勇","caseCreateTime":"2015年07月08日"}

class detail_obtain:
    def __init__(self, store_dir, log_dir, backup_dir):
       self.title = {'id':'id', 'caseCode':'案号', 'execCourtName':'执行法院', 'execMoney':'执行标的', \
       'partyCardNum':'身份证号码/组织机构代码', 'pname':'被执行人姓名/名称', 'caseCreateTime':'立案时间'}
       self.detail = {}
       self.store_dir = store_dir
       self.log_dir = log_dir
       self.backup_dir = backup_dir
       self.store_file = ""
       self.current_process_idx = 0
       self.proxies = {}
       self.range_start_id = 0
       self.range_end_id = 0
       self._load_conf()

    def _load_conf(self):
        cfg = ConfigParser()
        cfg.read('conf.ini')
        prox = cfg.get('proxies', 'proxies')
        prox = prox.split(',')
        proxies = {}
        for p in prox:
            p = p[p.find("\""):]
            key = p[1:p.find("\"", 1)]; #print(key)
            val = p[p.find(":"):]; #print(val)
            val = val[val.find("\""):]; #print(val)
            val = val[1:val.find("\"",1)]; print(val)
            proxies[key] = val 
        self.proxies = proxies
        #print(self.proxies)
        self.range_start_id = int(cfg.get('range', 'start_id'))
        self.range_end_id = int(cfg.get('range', 'end_id'))

    def item(self, attr, last=False):
        end_tag = "\n" if last else "\t"
        value = str(self.detail[attr]) if (attr == "execMoney" or attr == "id") else str(self.detail[attr].encode('utf-8'))
        return str(self.title[attr])+ ":" +value + str(end_tag)
        
    def parse(self, store_file):
        self.store_file = store_file
        out_line = self.item('pname')+self.item('partyCardNum')+self.item('execMoney')+self.item('caseCreateTime')+self.item('execCourtName')+self.item('caseCode')+self.item('id', True)
        with open(self.store_file, 'ab') as sf:
            sf.write(out_line)
            sf.flush()

    def crawl_by_range(self, start_id, end_id):
        range_tag = str(start_id) + "-" + str(end_id)
        store_file = self.store_dir + "/result." + range_tag
        log_file = self.log_dir + "/fail." + range_tag
        ISOTIMEFORMAT = '%Y-%m-%d-%X'
        cur_time = time.strftime(ISOTIMEFORMAT, time.localtime(time.time()))
        backup_file = self.backup_dir + "/reasult." + range_tag + "-" + cur_time
        if os.path.exists(store_file):
            with open(store_file, "r") as f:
                try:
                    f.seek(-512, 2)
                    lines = f.readlines()
                except:
                    f.seek(0, 0)
                    lines = f.readlines()
                finally:
                    shutil.move(store_file, backup_file)
                    last_id = lines[-1].split('\t')[-1].split(':')[-1]
                    start_id = int(last_id) + 1

        detail_url_prefix = "http://zhixing.court.gov.cn/search/detail?id="
        headers = {
            'User-Agent':'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:38.0) Gecko/20100101 Firefox/38.0'
        }
        for idx in range(start_id,  end_id):
            try:
                self.current_process_idx = idx
                with requests.Session() as s:
                    r = s.get(detail_url_prefix + str(idx), headers=headers, timeout=1)
                    print(r.status_code)
                    if r.status_code == 200:
                        self.detail = r.json().copy()
                        self.parse(store_file)
                    else:
                        with open(log_file, 'ab') as lf:
                            lf.write(str(idx) + "\n")
                            lf.flush()
            except:
                with open(log_file, 'ab') as lf:
                    lf.write(str(idx) + "\n")
                    lf.flush()
    
    def crawl(self, thread_tot=3):
        step = ((self.range_end_id - self.range_start_id + 1) / thread_tot) + 1
        pool = ThreadPoolExecutor(5)
        for idx in range(self.range_start_id, self.range_end_id, step):
            pool.submit(self.crawl_by_range, idx, idx + step)

def main():
    backup_dir = 'backup'
    store_dir = 'result'
    log_dir = 'log'
    if not os.path.exists(store_dir):
        os.mkdir(store_dir)
    if not os.path.exists(log_dir):
        os.mkdir(log_dir)
    if not os.path.exists(backup_dir):
        os.mkdir(backup_dir)
    detail_obtain(store_dir=store_dir, log_dir=log_dir, backup_dir=backup_dir).crawl()
        
if __name__ == '__main__':
    main()
