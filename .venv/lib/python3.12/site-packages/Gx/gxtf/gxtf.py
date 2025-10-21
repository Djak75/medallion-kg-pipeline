# -*- coding: utf-8 -*-
# @Author: Gx
# @Date:   2019-04-17 15:27:11
# @Last Modified by:   Administrator
# @Last Modified time: 2019-04-18 09:27:00
import tensorflow as tf
def add_layer(input, in_size, out_size, name="default",activation_func= None, Batch_Normalizaiton= False):
	'添加一层全连接层，输入维度，输出维度，name， 可选 激活函数和批标准化(BN)'
	Weights = tf.Variable(tf.random_normal([in_size,out_size]),dtype=tf.float32)
	Biases = tf.Variable(tf.random_normal([1, out_size]), dtype=tf.float32)
	result = tf.matmul(input, Weights) + Biases
	if Batch_Normalizaiton:
		result = BN(result, out_size)


	if activation_func is None:
		return result
	else:
		return eval(activation_func)(result)
def add_conv(input, kernel, strides= [1,1,1,1], padding= "SAME", filter_var="default"):
	'''
		输入数据[batch, height, width, channels], 
		kernel[kheight,kwidth, input_channel, out_channel],
		strides步长 这里我们只用了中间的两个值 [1, stride, stride, 1]
		padding string类型，值为“SAME” 和 “VALID”，表示的是卷积的形式，是否考虑边界。
							   "SAME"是考虑边界，不足的时候用0去填充周围，"VALID"则不考虑
		kernel输入需要变量 而不是 直接 传入列表要不会有问题，很奇怪，待日后解决，目前直接在代码中直接生成相应的变量
	'''
	if filter_var == "default":
		filter = tf.Variable(tf.random_normal(kernel, stddev=1.0), tf.float32)
	else:
		filter = filter_var
	return tf.nn.conv2d(input, filter, strides, padding)
def pooling(input, ksize=[1,2,2,1] , strides=[1,2,2,1] , padding="SAME" , poolop= "tf.nn.max_pool"):
	'''
		第一个参数value：需要做池化的输入图像，输入feaure map，因为池化在卷积后边。shape为[batch, in_height, in_width, in_channels]：
		batch：训练时一个batch的图片数量
		in_height：输入图像的高度
		in_width：输入图像的宽度
		in_channels：输入feature map的数量
		第二个参数ksize：类似于卷积的过滤器，池化窗口的大小，是一个长度为4的一维数组，但是这个数组的第一个和最后一数必须为1，即[1, height, width, 1]。这意味着池化层的过滤器是不可以在batch和channels上做池化。实际应用中，使用最多的过滤器尺寸为[1, 2, 2, 1]或者[1, 3, 3, 1]。

		height:过滤器的高度
		width：过滤器的宽度
		第三个参数strides：不同维度上的步长，是一个长度为4的一维向量，[ 1, strides, strides, 1]，第一维和最后一维的数字要求必须是1。因为卷积层的步长只对矩阵的长和宽有效。

		第四个参数padding：string类型，是否考虑边界，值为“SAME”和“VALID”，"SAME"是考虑边界，不足的时候用填充周围，"VALID"则不考虑边界。。
	'''
	# return poolop(input,)
	return eval(poolop)(input, ksize, strides, padding)
def BN(Wx_plus_b,output_size):
	'添加 Batch_Normalizaiton(批标准化)， 校正数据'
	fc_mean, fc_var = tf.nn.moments(Wx_plus_b, axes=[0])
	scale = tf.Variable(tf.ones([output_size]))
	shift = tf.Variable(tf.zeros([output_size]))
	epsilon = 0.001
	Wx_plus_b = tf.nn.batch_normalization(Wx_plus_b, fc_mean, fc_var, shift, scale, epsilon)
	return Wx_plus_b



def _t():
	print("this t ")
def _main():
	_t()

	pass
if __name__ == '__main__':
	_main()