'用于获取当前时间'
import time
def _getms():
	return str(int((time.time()%1) *1000))
def get_time(param):
	'''以时间戳的形式返回, param控制返回格式
		1 返回时间戳
		2 返回时间时间元组
		3 以字符串的形式返回YYYY-MM-DD-HH-MM-SS 格式
		4 在3的基础上输出毫秒信息
		5 待添加
	'''
	if param == 1 :
		return time.time()
	elif param == 2:
		return time.localtime()
	elif param == 3:
		return time.strftime('%Y_%m_%d_%Hh_%Mm_%Ss', time.localtime(time.time()))
	# import time
	elif param == 4:
		return time.strftime('%Y_%m_%d_%Hh_%Mm_%Ss', time.localtime(time.time()))+"_"+_getms()+"ms"
	else:
		print("param mistakes , please recheck it !")
		return ""

if __name__ == '__main__':
	print(get_time(4))