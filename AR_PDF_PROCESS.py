# -*- coding: utf-8 -*-
'''
from pdfminer.pdfinterp import PDFResourceManager, process_pdf, PDFPageInterpreter
from pdfminer.converter import TextConverter
from pdfminer.layout import LAParams
from io import open
from pdfminer.pdfparser import PDFParser,PDFDocument
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.converter import PDFPageAggregator
from pdfminer.layout import LTTextBoxHorizontal,LAParams
from pdfminer.pdfinterp import PDFTextExtractionNotAllowed
'''
import pdfplumber
import re
import pandas as pd
import os
import time
import requests
from urllib.request import urlopen
import csv
from io import StringIO
import shutil

def readPDF3(path, vocab, freq = False):
	#用于PDF离线文件：使用pdfpulmber来进行PDF文件解析
	pdf = pdfplumber.open(path,password='')
	pages_no = len(pdf.pages)
	vocab_count = [0]*len(vocab)
	print('Total Page : ',pages_no)
	if freq:
		for i in range(int(pages_no/3)):  
			page=pdf.pages[i]
			data=page.extract_text()
			for index,word in enumerate(vocab):
				t = data.count(word)
				if t>0:
					vocab_count[index]+=t
					print('Page',i+1,':',word,':',t)

	else:
		cc = 0
		for i in range(int(pages_no/3)):  
			page=pdf.pages[i]
			data=page.extract_text()
			for index,word in enumerate(vocab):
				if vocab_count[index]==0:
					try:
						if word in data:
							vocab_count[index]=1
							cc+=1
					except:
						pass
				else:
					pass
			if cc == len(vocab):
				break

	pdf.close()
	return vocab_count

#删除
def rm_file(path):
	'''
	if os.path.exists(TEMP_PATH):
		shutil.rmtree(TEMP_PATH, ignore_errors = True)
		os.makedirs(TEMP_PATH)
	'''
	for path_file in os.listdir(path):
		#print(path_file)
		os.remove(path+path_file)

def w3_Prase_PDF(df,vocab,freq,reloads):
	# 优点：对PDF文件进行有效解析，统计有效结果，异常编码不影响
	# 缺点：速度慢
	# 先进行PDF文件下载，然后解析PDF文件
	# 文件数量大于30时，不保留PDF源文件；小于30时保留源文件
	start = time.time()
	#tt_path = 'PDF_SOURCE/TMP30.pdf'
	rm_file('PDF_SOURCE/')
	LOG_FILE = "EXCEPTION_LOG/Fail_Praser_pdf.txt"
	info = open(LOG_FILE,'w')
	info.close()
	
	total = df.shape[0]
	vocab_list = []

	output_csv_file = 'RES/Annual_report_keyword_count.csv'
	#assert not os.path.exists(output_csv_file) 'The last RES/Annual_report_keyword_count.csv is exist!!! Please remove or save it before running the code!!!'
	if reloads[0] ==1:
		print('******** csv a+ mode ******** ')
		csv_out=open(output_csv_file, 'a+', newline='', encoding='gb18030')
		writer = csv.writer(csv_out)	
	else:
		print('******** csv w mode ******** ')
		csv_out=open(output_csv_file, 'w', newline='', encoding='gb18030')
		writer = csv.writer(csv_out)
		writer.writerow(['year', 'code', 'name']+vocab)

	for index, row in df.iterrows():
		#解析异常时候重启用,并从异常index50分段开始从新写入
		if reloads[0] ==1 and index < (reloads[1]//50)*50:
			continue
		filelink = row['file_link']
		print(index,':',filelink)
		try:
			response2 = requests.get(filelink)
			name = filelink.split('/')[-1]
			tt_path = 'PDF_SOURCE/'+name
			
			with open(tt_path, 'wb') as file:
				file.write(response2.content)
			file.close()
			
			vocab_count = readPDF3(tt_path,vocab,freq)
			print(vocab_count)
			row_new = [list(row[['year', 'code', 'name']])+vocab_count]
			writer.writerows(row_new)
			#if index == 16449:
			#	break
			if index % 50 == 0:
				csv_out.close()
				rm_file('PDF_SOURCE/')
				csv_out=open(output_csv_file, 'a+', newline='', encoding='gb18030')
				writer = csv.writer(csv_out)

		except Exception as e:
			print('\n**** process_pdf excepetion ****')
			print(row['code'],':',e)
			LOG_FILE = "EXCEPTION_LOG/Fail_Praser_pdf.txt"
			info = open(LOG_FILE,'a')
			info.write(str(row['code'])+'  :  '+row['file_name']+'   '+row['file_link']+'   '+str(e)+'\n')
			info.close()
			
			csv_out.close()
			csv_out=open(output_csv_file, 'a+', newline='', encoding='gb18030')
			writer = csv.writer(csv_out)	
		
	csv_out.close()
	end = time.time()
	print('====>  Praser PDF and count keyword consume {} seconds'.format(end-start))

def Process_all(vocab=[''],freq=False,reloads =[0,0]):
	path='LL/NB/annual_report_final.csv'
	if os.path.exists('PDF_SOURCE'):
		print('PDF SORUCE EXISTS!!!')
	else:
		print('mkdir PDF SORUCE ')
		os.mkdir('PDF_SOURCE')

	df = pd.read_csv(path, encoding = 'utf-8')
	df = df[df.year>=2017]
	print('  Start processing annual report pdf file, total :',df.shape[0])
	#w1_Download_And_Prase_PDF(path,vocab)
	w3_Prase_PDF(df,vocab,freq,reloads)

def Process_file():
	filename = 'file_input.txt'
	output_csv_file = 'RES/Annual_report_keyword_count by file_input.csv'
	LOG_FILE = "EXCEPTION_LOG/Fail_Praser_pdf.txt"
	info = open(LOG_FILE,'w')
	info.close()
	rm_file('PDF_SOURCE/')
	
	E_tmp=[]
	count = 0
	with open(filename, 'r') as file_to_read:
		while True:
			count+=1
			lines = file_to_read.readline().strip('\n') # 整行读取数据
			if not lines:
				break
			if count == 1:
				lines = [str(i) for i in lines.split('/')]
			elif count == 2:
				if lines == '0':
					lines = False
				else:
					lines = True
			E_tmp.append(lines)
	vocab=E_tmp[0]
	freq=E_tmp[1]
	stock_code_set=E_tmp[2:]

	print('THE INPUT IS===> \n   keyword : {}\n   freq : {}\n   PDF filename : {}\n'\
		.format(vocab,freq,stock_code_set))

	print('Start processing annual report pdf file, total :',len(stock_code_set)-2)
	
	csv_out=open(output_csv_file, 'w', newline='', encoding='gb18030')
	writer = csv.writer(csv_out)
	writer.writerow(['filelink']+vocab)

	for index,filelink in enumerate(stock_code_set):
		print(index,':',filelink)
		try:
			response2 = requests.get(filelink)
			name = filelink.split('/')[-1]
			tt_path = 'PDF_SOURCE/'+name
			
			with open(tt_path, 'wb') as file:
				file.write(response2.content)
			file.close()
			
			vocab_count = readPDF3(tt_path,vocab,freq)
			print(vocab_count)
			row_new = [[filelink]+vocab_count]
			writer.writerows(row_new)

		except Exception as e:
			print('\n**** process_pdf excepetion ****')
			print(row['code'],':',e)
			LOG_FILE = "EXCEPTION_LOG/Fail_Praser_pdf.txt"
			info = open(LOG_FILE,'a')
			info.write(file_link+'  :  '+str(e)+'\n')
			info.close()

			csv_out.close()
			csv_out=open(output_csv_file, 'a+', newline='', encoding='gb18030')
			writer = csv.writer(csv_out)	
	csv_out.close()
	print('====>  Praser PDF and count keyword is finish.')
	return u'Praser PDF and count keyword is finish'


if __name__ == '__main__':
	keyword_csv_file = 'RES/Annual_report_keyword_count by file_input.csv'
	assert not os.path.exists(keyword_csv_file),'The last RES/Annual_report_keyword_count by file_input.csv is exist!!! Please remove or save it before running the code!!!'
	
	Process_file()
	#Process_all(['大数据'])



