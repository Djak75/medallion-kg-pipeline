import tkinter as tk
from tkinter import filedialog
import os
import sys
def get_One_File(filetypes = [("text file", "*.txt"),("image file", ("*.jpg","*.png","*.svg")),("all","*.*")]):
	'基于tkinter 获取文件， 默认为*.txt,或*.*'
	# 获取一个文件
	root = tk.Tk()
	root.withdraw()
	file_path = filedialog.askopenfilename(filetypes=filetypes, initialdir = os.getcwd())
	if file_path=="":
		return False, ""
	else:
		return True, file_path

def _get_Multi_file():
	'同时获取多个文件，功能待完善'
	root = tk.Tk()
	root.withdraw() 
	file_path = filedialog.askopenfilenames()
	for f in file_path:
		fo = f.split('.')[0]+'.csv'
		with open(fo,'w') as foo:
			with open(f,'r') as fn:
				fn.readline()
				for line in fn.readlines():
					li = line.strip().split()
					foo.write('%f,%f\n'%(float(li[1]),float(li[0])))
					print(li)
def get_Folder():
	'获取一个文件夹路径，第一个返回值是flag 表示是否获取正确 的路径,第二个返回值是path'
	root = tk.Tk()
	root.withdraw()
	path_="myname"
	path_ = filedialog.askdirectory(initialdir = os.getcwd())
	if path_=="":
		return False, ""
	else:
		return True, path_

if __name__ == '__main__':
	# main()
	flag , path_= get_One_File()
	if flag:
		print(path_)
	else:
		print("path wrong")
	flag , path_= get_Folder()
	if flag:
		print(path_)
	else:
		print("path wrong")