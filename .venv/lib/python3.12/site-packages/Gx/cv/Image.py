'Gx 包下的图片处理'

def read_img(filename):
	import cv2
	import np as np
	global cv2
	global np
	'使用 imdecode方法能够访问到包含中文字符的图片'
	try:
		arr = cv2.imdecode(np.fromfile(filename, dtype= np.uint8), -1)
	except:
		print("error with filename")
	return np.array(arr)
	pass
def write_img(dir, frame):
	import cv2
	import np as np
	global cv2
	global np
	'使用 cv2 imencode 方法写入图片，防止出现中文字符的问题'
	try:
		suffix = "."+dir.strip().split(".")[-1]
		cv2.imencode(suffix, frame)[1].tofile(dir)
		return True
	except:
		print("errors result")
		return  False
	pass

def _main():
	arr = read_img("./image.jpg")
	print(arr)
	write_img("./my.seg",arr)
	print(arr.shape)
	pass
if __name__ == '__main__':
	_main()

