# -*- coding: utf-8 -*-
import pandas as pd
import os

from AR_GET_FILELINK import process_by_mon
from AR_PDF_PROCESS import Process_all

filename = 'input.txt'
# Input:
# 	E_tmp = [start_year,end_year,stack_code_set,plate,keyword,freq]

E_tmp=[]
count = 0
with open(filename, 'r') as file_to_read:
	while True:
		count+=1
		lines = file_to_read.readline().strip('\n') # 整行读取数据

		if not lines:
			break
		#print(lines)
		if count ==1:
			if lines[-1]!=';':
				lines+=';'
		elif count in [4,5]:
			if lines == '/':
				lines =['']
			else:
				lines = [str(i) for i in lines.split('/')]
		elif count == 6:
			if lines == '0':
				lines = False
			else:
				lines = True
		elif count ==7:
			lines = [int(i) for i in lines.split('/')]
		#print(lines)
		E_tmp.append(lines)
file_to_read.close()

# 板块类型：沪市：shmb；深市：szse；深主板：szmb；中小板：szzx；创业板：szcy；
plate = E_tmp[0]
# 年报开始和结束日期
start_year = E_tmp[1]
end_year = E_tmp[2]  
# 股票列表：[''] 表示所有股票 
stock_code_set=['']
stock_code_set=E_tmp[3]  
# 关键词
keyword = E_tmp[4]
# 是否获取词频统计
freq = E_tmp[5]
# 是否获取词频统计
Parase_reload = E_tmp[6]
print('THE INPUT IS===> \n   plate : {}\n   start_year : {}\n   end_year : {}\n   stock_code_set : {}\n   keyword : {}\n   freq : {}\n   Parase_reload : {}\n'\
		.format(plate,start_year,end_year,stock_code_set,keyword,freq,Parase_reload))
#检查之前的年报关键词统计文件是否存在

if Parase_reload[0]==0:
	pdf_filelink_csv_file='ANNUAL_REPORT/annual_report_final.csv'
	assert not os.path.exists(pdf_filelink_csv_file),'The last ANNUAL_REPORT/annual_report_final.csv is exist!!! Please remove or save it before running the code!!!'
	keyword_csv_file = 'RES/Annual_report_keyword_count.csv'
	assert not os.path.exists(keyword_csv_file),'The last RES/Annual_report_keyword_count.csv is exist!!! Please remove or save it before running the code!!!'
	print('>>>>>>>>>>>>>>>>>>>>>             Start getting the Annual report pdf filelink from http://www.cninfo.com.cn/ !!!          <<<<<<<<<<<<<<<<<<<<<')
	process_by_mon(start_year,end_year,stock_code_set,plate)
print('\n\n>>>>>>>>>>>>>>>>>>>>>             Start processing Annual report pdf file !!!          <<<<<<<<<<<<<<<<<<<<<')
Process_all(keyword,freq,Parase_reload)

