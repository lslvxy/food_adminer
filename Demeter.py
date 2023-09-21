# -*- coding: utf-8 -*-

################################################################################
## Form generated from reading UI file 'user_password_request.ui'
##
## Created by: Qt User Interface Compiler version 6.5.0
##
## WARNING! All changes made in this file will be lost when recompiling UI file!
################################################################################
import logging
import sys
import traceback

from PySide6 import QtGui, QtCore

from utils import parse
from foodgrabV2 import parse_foodgrabV2
from foodpandaV2 import parse_foodpandaV2
from PySide6.QtCore import (QCoreApplication, QDate, QDateTime, QLocale,
                            QMetaObject, QObject, QPoint, QRect,
                            QSize, QTime, QUrl, Qt, QEventLoop, QTimer)
from PySide6.QtGui import (QBrush, QColor, QConicalGradient, QCursor,
                           QFont, QFontDatabase, QGradient, QIcon,
                           QImage, QKeySequence, QLinearGradient, QPainter,
                           QPalette, QPixmap, QRadialGradient, QTransform, QWindow)
from PySide6.QtWidgets import (QApplication, QButtonGroup, QLabel, QPushButton,
                               QRadioButton, QSizePolicy, QTextBrowser, QTextEdit,
                               QWidget, QListWidget, QVBoxLayout, QLineEdit, QMessageBox)


class Ui_Widget(object):
    def setupUi(self, Widget):
        if not Widget.objectName():
            Widget.setObjectName(u"Widget")
        Widget.resize(800, 600)
        Widget.setAutoFillBackground(False)
        self.lineEdit = QLineEdit(Widget)
        self.lineEdit.setObjectName(u"lineEdit")
        self.lineEdit.setGeometry(QRect(110, 50, 621, 41))
        # self.lineEdit.setText(
        #     u"https://food.grab.com/sg/en/restaurant/mcdonald-s-jurong-green-cc-delivery/SGDD04996")
        # self.lineEdit.setText(
        #     u"https://www.foodpanda.hk/restaurant/v3iw/bakeout-homemade-koppepan")
        self.label = QLabel(Widget)
        self.label.setObjectName(u"label")
        self.label.setGeometry(QRect(40, 60, 58, 16))
        self.radioButton = QRadioButton(Widget)
        self.buttonGroup = QButtonGroup(Widget)
        self.buttonGroup.setObjectName(u"buttonGroup")
        self.buttonGroup.addButton(self.radioButton)
        self.radioButton.setObjectName(u"EN")
        self.radioButton.setGeometry(QRect(110, 120, 99, 20))
        self.radioButton_2 = QRadioButton(Widget)
        self.buttonGroup.addButton(self.radioButton_2)
        self.radioButton_2.setObjectName(u"TH")
        self.radioButton_2.setGeometry(QRect(320, 120, 99, 20))
        self.radioButton_3 = QRadioButton(Widget)
        self.buttonGroup.addButton(self.radioButton_3)
        self.radioButton_3.setObjectName(u"CN")
        self.radioButton_3.setGeometry(QRect(550, 120, 99, 20))
        self.label_2 = QLabel(Widget)
        self.label_2.setObjectName(u"label_2")
        self.label_2.setGeometry(QRect(30, 120, 71, 16))
        self.textBrowser = QTextBrowser(Widget)
        self.textBrowser.setObjectName(u"textBrowser")
        self.textBrowser.setGeometry(QRect(110, 160, 621, 321))
        self.pushButton = QPushButton(Widget)
        self.pushButton.setObjectName(u"pushButton")
        self.pushButton.setGeometry(QRect(110, 510, 621, 61))

        self.retranslateUi(Widget)

        QMetaObject.connectSlotsByName(Widget)

    # setupUi

    def retranslateUi(self, Widget):
        Widget.setWindowTitle(QCoreApplication.translate("Demeter", u"Demeter", None))
        self.label.setText(QCoreApplication.translate("Widget", u"URL", None))
        self.radioButton.setText(QCoreApplication.translate("Widget", u"EN", None))
        self.radioButton_2.setText(QCoreApplication.translate("Widget", u"TH", None))
        self.radioButton_3.setText(QCoreApplication.translate("Widget", u"CN", None))
        self.label_2.setText(QCoreApplication.translate("Widget", u"Language", None))
        self.pushButton.setText(QCoreApplication.translate("Widget", u"Start", None))


class EmittingStr(QtCore.QObject):
    textWritten = QtCore.Signal(str)

    def write(self, text):
        self.textWritten.emit(str(text))
        loop = QEventLoop()
        QTimer.singleShot(100, loop.quit)
        loop.exec()
        QApplication.processEvents()


class mywindow(QWidget, Ui_Widget):
    url = ""
    language = ""

    def __init__(self):
        super(mywindow, self).__init__()
        self.setupUi(self)
        self.ListWidget = QListWidget()
        self.mainLayout = QVBoxLayout()
        # self.pushButton.clicked.connect(self.loginFuc)
        sys.stdout = EmittingStr()
        sys.stdout.textWritten.connect(self.output_written)
        self.pushButton.clicked.connect(self.loginFuc)
        self.lineEdit.textChanged.connect(self.loginFuc2)

    def output_written(self, text):
        cursor = self.textBrowser.textCursor()
        cursor.movePosition(QtGui.QTextCursor.End)
        cursor.insertText(text)
        self.textBrowser.setTextCursor(cursor)
        self.textBrowser.ensureCursorVisible()

    def loginFuc2(self):
        page_url = self.lineEdit.text()
        variables = parse(page_url)
        if variables == {}:
            # self.a = 'Unsupported page addresses'
            # self.ListWidget.addItem(self.a)
            print('Unsupported page addresses')
        else:
            print('Language is ' + variables.get("language"))
            radio = self.findChild(QRadioButton, variables.get("language"))
            if radio:
                radio.setChecked(True)
            else:
                print('Use default Language EN')
                self.findChild(QRadioButton, "EN").setChecked(True)

    def loginFuc(self):
        # msgBox = QMessageBox(self)
        # msgBox.setWindowTitle("Demeter")
        language = self.buttonGroup.checkedButton().text()
        page_url = self.lineEdit.text()
        self.textBrowser.clear()

        variables = parse(page_url)
        if variables == {}:
            # self.a = 'Unsupported page addresses'
            # self.ListWidget.addItem(self.a)
            print('Unsupported page addresses')
        else:
            self.pushButton.setDisabled(True)
            variables['language'] = language
            print('Language is ' + language)
            if variables.get('type') == 'foodgrab':

                # self.a = parse_foodgrabV2(page_url, variables)
                # for i in self.a:
                #     self.ListWidget.addItem(i)
                # self.mainLayout.addWidget(self.ListWidget)
                # self.setLayout(self.mainLayout)
                try:
                    parse_foodgrabV2(page_url, variables)
                    print("Collection complete")
                except Exception as e:
                    print('Collection Fail!')
                    print("Collection Exception: %s" % e)
                    traceback.print_exc()
                    self.pushButton.setDisabled(False)
                    # msgBox.exec()

            elif variables.get('type') == 'foodpanda':

                # //self.a = parse_foodpanda(page_url, variables)
                # //for i in self.a:
                #     self.ListWidget.addItem(i)
                # self.mainLayout.addWidget(self.ListWidget)
                # self.setLayout(self.mainLayout)
                try:
                    parse_foodpandaV2(page_url, variables)
                    print("Collection complete")
                except Exception as e:
                    print('Collection Fail!')
                    print("Collection Exception: %s" % e)
                    traceback.print_exc()
                    self.pushButton.setDisabled(False)
                    # msgBox.exec()
            self.pushButton.setDisabled(False)

        #         print(variables)


if __name__ == '__main__':
    app = QApplication([])
    window = mywindow()
    window.show()
    app.exec()
