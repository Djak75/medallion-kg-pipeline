import os
import cv2

def debug(func):
	def work(*args, **kwargs):
		try:
			print("_debug")
			func(*args, **kwargs)
		except:
			print("except exit")
	return work
@debug
def video_display(path):
	'single video display: 播放当前path的video'
	video_capture = cv2.VideoCapture(path)
	fps = video_capture.get(cv2.CAP_PROP_FPS)
	flag, frame = video_capture.read()
	winname = ""
	try:
		if len(path.split("/")) == 1:
			winname= path.split(".")[0]
		else:
			winname= path.split("/")[-1].split(".")[0]
	except:
		print("some wrong may result, program exit with 0")
	while flag:
		cv2.imshow(winname, frame)
		flag, frame = video_capture.read()

		KeyBoardListenser = cv2.waitKey(10)
		if KeyBoardListenser == 27: #esc 
			break		
	cv2.destroyAllWindows()
	print("finished")		