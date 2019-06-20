# coding = utf-8

import csv
import math
import os
import time
import requests
import random
import re
import pandas as pd
import json
'''
START_DATE = '2015-01-01'  # 搜索的起始日期
END_DATE = str(time.strftime('%Y-%m-%d'))  # 默认当前提取，可设定为固定值
END_DATE = '2015-01-01'
OUT_DIR = 'D:/PY/LL'
'''
# 前面的一些参数PLATE CATEGORY等是向服务器请求时要发送过去的参数。
OUTPUT_FILENAME = 'annual_report'
# 板块类型：沪市：shmb；深市：szse；深主板：szmb；中小板：szzx；创业板：szcy；
global PLATE #= 'szse;'
# 公告类型：category_scgkfx_szsh（首次公开发行及上市）、category_ndbg_szsh（年度报告）、category_bndbg_szsh（半年度报告）
CATEGORY = 'category_ndbg_szsh;'

URL = 'http://www.cninfo.com.cn/new/hisAnnouncement/query'
# 使用浏览器代理，作用聊胜于无，一般需要更换代理IP来进行
HEADER = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36',
    'X-Requested-With': 'XMLHttpRequest'
}

MAX_PAGESIZE = 50
MAX_RELOAD_TIMES = 5
RESPONSE_TIMEOUT = 10

error_log = 'EXCEPTION_LOG/get_filelink_error.log'

def standardize_dir(dir_str):
    assert (os.path.exists(dir_str)), 'Such directory \"' + str(dir_str) + '\" does not exists!'
    if dir_str[-1] != '/':
        return dir_str + '/'
    else:
        return dir_str

# 参数：页面id(每页条目个数由MAX_PAGESIZE控制)，是否返回条目数(bool)
def get_response(page_num,stack_code,return_total_count=False,START_DATE = '2017-01-01',END_DATE = '2018-01-01'):
    # stack_code 不给定的话就是全部的年报数据信
    # 2017-01-01能给定2016的年报,其原因在于年报文件发布时间
    # 这里就是 body 信息
    query = {
        'stock': stack_code,
        'searchkey': '',
        'plate': '',
        'category': CATEGORY,
        'trade': '',
        'column': '', #注意沪市为sse
#        'columnTitle': '历史公告查询',
        'pageNum': page_num,
        'pageSize': MAX_PAGESIZE,
        'tabName': 'fulltext',
        'sortName': '',
        'sortType': '',
        'limit': '',
        'showTitle': '',
        'seDate': START_DATE + '~' + END_DATE,
    }
    result_list = []
    excepiton_list = []
    reloading = 0
    while True:
        reloading += 1
        if reloading > MAX_RELOAD_TIMES:
            return []
        elif reloading > 1:
            time.sleep(random.randint(5, 10))
            print('... reloading: the ' + str(reloading) + ' round ...')
        
        try:
            r = requests.post(URL, query, HEADER, timeout=RESPONSE_TIMEOUT)
            #print(r.json()) #Expecting value: line 1 column 1 (char 0)
            print(r.status_code)
            #print(r.text)
        except Exception as e:
            print(e)
            continue
        if r.status_code == requests.codes.ok and r.text != '':
            break

	# 以下就是开始解析 json 数据，和解析字典类似
    my_query = r.json()

    try:
        r.close()
    except Exception as e:
        print(e)
    if return_total_count:
        return my_query['totalRecordNum']
    else:
        for each in my_query['announcements']:
            file_link = 'http://static.cninfo.com.cn/' + str(each['adjunctUrl'])

            file_name = __filter_illegal_filename(
                str(each['announcementTitle']) + '_'  + '(' + str(each['adjunctSize'])  + 'k).' + file_link.split('.')[-1])

            if file_name.endswith('.PDF') or file_name.endswith('.pdf'):
                if '取消' not in file_name and '摘要' not in file_name and '年度报告' in file_name and '更正' not in file_name and '英文' not in file_name and '补充' not in file_name:
                    try:
                        year = 	re.findall(r"\d+",str(each['announcementTitle']))[0] #str(each['announcementTitle'])[:4]
                    except Exception as e:
                        print('//// Year exception: ' , str(each['announcementTitle']))
                        year='3000'
                    code = PLATE[:2] + str(each['secCode'])
                    name = str(each['secName'])   
                    result_list.append([year, code, name, file_name, file_link])      
        return result_list

def __log_error(err_msg):
    err_msg = str(err_msg)
    print(err_msg)
    with open(error_log, 'a', encoding='gb18030') as err_writer:
        err_writer.write(err_msg + '\n')

def __filter_illegal_filename(filename):
    illegal_char = {
        ' ': '',
        '*': '',
        '/': '-',
        '\\': '-',
        ':': '-',
        '?': '-',
        '"': '',
        '<': '',
        '>': '',
        '|': '',
        '－': '-',
        '—': '-',
        '（': '(',
        '）': ')',
        'Ａ': 'A',
        'Ｂ': 'B',
        'Ｈ': 'H',
        '，': ',',
        '。': '.',
        '：': '-',
        '！': '_',
        '？': '-',
        '“': '"',
        '”': '"',
        '‘': '',
        '’': ''
    }
    for item in illegal_char.items():
        filename = filename.replace(item[0], item[1])
    return filename

def get_url(OUT_DIR,stack_code_set,START_DATE,END_DATE):
    #START_DATE=START_DATE+'-07-01'
    #END_DATE=END_DATE+'-01-01'

    output_csv_file = OUT_DIR + OUTPUT_FILENAME.replace('/', '') + '_' + START_DATE.replace('-', '') + '-' + END_DATE.replace('-', '') + '.csv'
    #with open(output_csv_file, 'w', newline='', encoding='gb18030') as csv_out:
    csv_out=open(output_csv_file, 'w', newline='', encoding='gb18030')
    writer = csv.writer(csv_out)
    writer.writerow(['year', 'code', 'name', 'file_name', 'file_link'])
    #stack_code_set=['000002','000004','000005','000006','000007','000008']
    
    start=time.time()
    
    for stack_code in stack_code_set:
        # 获取记录数、页数
        item_count = get_response(1, stack_code,True,START_DATE = START_DATE,END_DATE = END_DATE)
        assert (item_count != []), 'Please restart this script!'
        begin_pg = 1
        end_pg = int(math.ceil(item_count / MAX_PAGESIZE))
        print('Page count: ' + str(end_pg) + '; item count: ' + str(item_count) + '.')
        time.sleep(2)
    
        # 逐页抓取
        #with open(output_csv_file, 'w', newline='', encoding='gb18030') as csv_out:
            #writer = csv.writer(csv_out)
        for i in range(begin_pg, end_pg + 1):
            row = get_response(i,stack_code,START_DATE = START_DATE,END_DATE = END_DATE)
            #print(' ===> Annual report(pdf): {} '.format(len(row)))
            if not row:

                __log_error('Failed to fetch page #' + str(i) +
                            ': exceeding max reloading times (' + str(MAX_RELOAD_TIMES) + ').')
                #print(row)
                continue
            else:
                print(' ===> Annual report(pdf): {} '.format(len(row)))
                writer.writerows(row)                
                last_item = i * MAX_PAGESIZE if i < end_pg else item_count
                print('Page ' + str(i) + '/' + str(end_pg) + ' fetched, it contains items: (' +
                      str(1 + (i - 1) * MAX_PAGESIZE) + '-' + str(last_item) + ')/' + str(item_count) + '.')
            SLT = random.uniform(2,4)
            time.sleep(SLT)
    csv_out.close()    
    end=time.time()
    #print('\n time to open processing all files are {}*********\n'.format((end-start)))
    
    return output_csv_file

def process_by_mon(start_date,end_date,stock_list = [''], plate = 'szse;'):
    OUT_DIR = 'ANNUAL_REPORT/'
    out_dir = standardize_dir(OUT_DIR)

    PLATE=plate
    date_list=['-01-01','-02-01','-03-01','-04-01','-05-01','-06-01','-07-01','-08-01','-09-01','-10-01','-11-01','-12-01','-01-01']

    for start in range(start_date, end_date):
        for i in range(len(date_list)-1):
            START_DATE = str(start) + date_list[i]
            if i == 11:
                END_DATE = str(start+1) + date_list[i+1]
            else:
                END_DATE = str(start) + date_list[i+1]
            print('\n******** Processing files from {} to {} ******** '.format(START_DATE ,END_DATE))
            output_csv_file_path=get_url(out_dir,stack_code_set,START_DATE,END_DATE)   # no returns

    print('\n******** Finish processing all files *********')

    print('\n******** Concat all csv files *********')
    files = os.listdir(out_dir)  
    print(files)
    csv_list = []
    for file in files:
        file = out_dir + file
        if os.path.splitext(file)[1] == ".csv":
            tmp = pd.read_csv(file, encoding = 'gb18030')
            csv_list.append(tmp)
    annual_report_final = pd.concat(csv_list, axis = 0)
    annual_report_final.to_csv(out_dir+'annual_report_final.csv',index = False)
    print('\n******** Finish getting the donwload filelink of Annual report pdf from url *********')


if __name__ == '__main__':

    stack_code_set=['000002','000004']
    stack_code_set=['']

    start_year = 2011
    end_year = 2019   
    #process_by_mon(start_year,end_year,stack_code_set)







	
