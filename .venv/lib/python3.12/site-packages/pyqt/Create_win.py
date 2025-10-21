import sys

from PyQt5.QtCore import QSize, Qt
from PyQt5.QtGui import QIcon
from PyQt5.QtWidgets import QApplication, QWidget, QPushButton, QHBoxLayout, QVBoxLayout, QLabel
from qtpy import QtWidgets


def create_win(title="window", size=(1920,1080),move = (0,0)):
    '''
    create a window
    :param title: 窗口的标题名字
    :param size:  一个元组 用于定于windows的大小,
    :param move:  move元组相比于原来windows的位置
    :return: 
    '''
    app = QApplication(sys.argv)
    w = QWidget()
    w.resize(size[0], size[1])
    w.move(move[0],move[1])

    w.setWindowTitle(title)
    w.show()
    sys.exit(app.exec_())

def create_win2(title="window", size=(1920,1080),move = (0,0)):
    '''
    create a window
    :param title: 窗口的标题名字
    :param size:  一个元组 用于定于windows的大小,
    :param move:  move元组相比于原来windows的位置
    :return:
    '''
    app = QApplication(sys.argv)
    w = QWidget()
    w.resize(size[0], size[1])
    w.move(move[0],move[1])
    w.setGeometry(300,300,300,200)
    btn = QPushButton('Buttion')

    w.setWindowTitle(title)
    w.setWindowIcon(QIcon('image.jpg'))
    w.show()
    sys.exit(app.exec_())

def t3():
    w = QWidget()
    w.setMinimumSize(QSize(1200, 600))
    layout = QHBoxLayout()
    layout2 = QHBoxLayout()

    layout_s = QVBoxLayout()
    btnW = QtWidgets.QWidget()

    ImageWidget = QtWidgets.QWidget()

    btn = QPushButton()
    # btn.clicked.connect()
    btn.setText("load_image")
    layout.addWidget(btn, alignment=Qt.AlignTop)

    btn_2 = QPushButton()
    # btn_2.clicked.connect(self.load_text)
    btn_2.setText("beauty")
    layout.addWidget(btn_2, alignment=Qt.AlignTop)
    btnW.setLayout(layout)

    label = QLabel()
    layout2.addWidget(label, alignment=Qt.AlignVCenter)

    label2 = QLabel()
    layout2.addWidget(label2, alignment=Qt.AlignTop)
    ImageWidget.setLayout(layout2)
    layout_s.addWidget(btnW)
    layout_s.addWidget(ImageWidget, alignment=Qt.AlignTop)

    w.setWindowTitle("beauty")

    w.setLayout(layout_s)
if __name__ == '__main__':
    t3()