# -*- coding: utf-8 -*-

################################################################################
## Form generated from reading UI file 'user_password_request.ui'
##
## Created by: Qt User Interface Compiler version 6.5.0
##
## WARNING! All changes made in this file will be lost when recompiling UI file!
################################################################################
import logging
import os
import pathlib
import sys
import traceback

from PySide6 import QtGui, QtCore

from ui_dialog import Ui_Dialog
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
                               QRadioButton, QSizePolicy, QTextBrowser, QTextEdit, QFileDialog,
                               QWidget, QListWidget, QVBoxLayout, QLineEdit, QMessageBox, QCheckBox, QPlainTextEdit,
                               QSpinBox, QComboBox, QCommandLinkButton, QDialogButtonBox, QDialog)
import pandas as pd

from PySide6.QtCore import (QCoreApplication, QDate, QDateTime, QLocale,
                            QMetaObject, QObject, QPoint, QRect,
                            QSize, QTime, QUrl, Qt)
from PySide6.QtGui import (QBrush, QColor, QConicalGradient, QCursor,
                           QFont, QFontDatabase, QGradient, QIcon,
                           QImage, QKeySequence, QLinearGradient, QPainter,
                           QPalette, QPixmap, QRadialGradient, QTransform)
from PySide6.QtWidgets import (QApplication, QCheckBox, QComboBox, QLabel,
                               QMainWindow, QPushButton, QSizePolicy, QSpinBox,
                               QTextBrowser, QTextEdit, QWidget)


class Ui_MainWindow(object):
    def setupUi(self, MainWindow):
        if not MainWindow.objectName():
            MainWindow.setObjectName(u"MainWindow")
        MainWindow.resize(800, 600)
        self.centralwidget = QWidget(MainWindow)
        self.centralwidget.setObjectName(u"centralwidget")
        self.textBrowser = QTextBrowser(self.centralwidget)
        self.textBrowser.setObjectName(u"textBrowser")
        self.textBrowser.setGeometry(QRect(100, 180, 600, 251))
        self.label_2 = QLabel(self.centralwidget)
        self.label_2.setObjectName(u"label_2")
        self.label_2.setGeometry(QRect(40, 130, 58, 16))
        self.label = QLabel(self.centralwidget)
        self.label.setObjectName(u"label")
        self.label.setGeometry(QRect(40, 40, 58, 16))
        self.spinBox = QSpinBox(self.centralwidget)
        self.spinBox.setObjectName(u"spinBox")
        self.spinBox.setGeometry(QRect(371, 460, 51, 30))
        self.spinBox.setMinimum(1)
        self.label_3 = QLabel(self.centralwidget)
        self.label_3.setObjectName(u"label_3")
        self.label_3.setGeometry(QRect(310, 460, 58, 30))
        self.btn_start = QPushButton(self.centralwidget)
        self.btn_start.setObjectName(u"btn_start")
        self.btn_start.setGeometry(QRect(100, 510, 600, 60))
        self.checkBox_proxy = QCheckBox(self.centralwidget)
        self.checkBox_proxy.setObjectName(u"checkBox_proxy")
        self.checkBox_proxy.setGeometry(QRect(100, 460, 85, 30))
        self.textEdit_input = QTextEdit(self.centralwidget)
        self.textEdit_input.setObjectName(u"textEdit_input")
        self.textEdit_input.setGeometry(QRect(100, 10, 600, 80))
        self.textEdit_input.setAcceptRichText(False)
        self.label_4 = QLabel(self.centralwidget)
        self.label_4.setObjectName(u"label_4")
        self.label_4.setGeometry(QRect(520, 460, 81, 30))
        self.comboBox_export = QComboBox(self.centralwidget)
        self.comboBox_export.addItem("")
        self.comboBox_export.addItem("")
        self.comboBox_export.addItem("")
        self.comboBox_export.setObjectName(u"comboBox_export")
        self.comboBox_export.setGeometry(QRect(600, 460, 103, 30))
        self.btn_file = QPushButton(self.centralwidget)
        self.btn_file.setObjectName(u"btn_file")
        self.btn_file.setGeometry(QRect(600, 120, 100, 40))
        self.lineEdit_filepath = QLineEdit(self.centralwidget)
        self.lineEdit_filepath.setObjectName(u"lineEdit_filepath")
        self.lineEdit_filepath.setGeometry(QRect(100, 120, 450, 40))
        MainWindow.setCentralWidget(self.centralwidget)

        self.retranslateUi(MainWindow)
        # self.textEdit_input.textChanged.connect(self.lineEdit_filepath.clear)
        self.lineEdit_filepath.textChanged.connect(self.textEdit_input.clear)

        QMetaObject.connectSlotsByName(MainWindow)

    # setupUi

    def retranslateUi(self, MainWindow):
        MainWindow.setWindowTitle(QCoreApplication.translate("MainWindow", u"Adminer", None))
        self.label_2.setText(QCoreApplication.translate("MainWindow", u"File", None))
        self.label.setText(QCoreApplication.translate("MainWindow", u"URL", None))
        self.label_3.setText(QCoreApplication.translate("MainWindow", u"Interval", None))
        self.btn_start.setText(QCoreApplication.translate("MainWindow", u"Start", None))
        self.checkBox_proxy.setText(QCoreApplication.translate("MainWindow", u"Use Proxy", None))
        self.label_4.setText(QCoreApplication.translate("MainWindow", u"Export Excel", None))
        self.comboBox_export.setItemText(0, QCoreApplication.translate("MainWindow", u"None", None))
        self.comboBox_export.setItemText(1, QCoreApplication.translate("MainWindow", u"Current", None))
        self.comboBox_export.setItemText(2, QCoreApplication.translate("MainWindow", u"All", None))

        self.btn_file.setText(QCoreApplication.translate("MainWindow", u"Choose File", None))

    # retranslateUi


class EmittingStr(QtCore.QObject):
    textWritten = QtCore.Signal(str)

    def write(self, text):
        self.textWritten.emit(str(text))
        loop = QEventLoop()
        QTimer.singleShot(100, loop.quit)
        loop.exec()
        QApplication.processEvents()


def save_log(all_log, batch_no):
    homedir = str(pathlib.Path.home())
    dir_path = os.path.join(homedir, "Aim_menu", "log")
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    if all_log:
        with open(os.path.join(dir_path, f'{batch_no}.log'), 'w') as f:
            f.write(all_log)
    pass


class MainWindow(QMainWindow):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.ui = Ui_MainWindow()
        self.ui.setupUi(self)
        sys.stdout = EmittingStr()
        sys.stdout.textWritten.connect(self.output_written)
        self.ui.btn_start.clicked.connect(self.loginFuc)
        self.ui.checkBox_proxy.stateChanged.connect(self.proxyChange)
        self.ui.btn_file.clicked.connect(self.chooseFile)

    url = ""
    language = ""

    def output_written(self, text):
        cursor = self.ui.textBrowser.textCursor()
        cursor.movePosition(QtGui.QTextCursor.End)
        cursor.insertText(text)
        self.ui.textBrowser.setTextCursor(cursor)
        self.ui.textBrowser.ensureCursorVisible()

    def proxyChange(self):
        checked = self.ui.checkBox_proxy.checkState()
        if checked == Qt.Checked:
            dlg = ProxyDlg(self)
            if dlg.exec():
                url = dlg.lineEdit_url.text()
                username = dlg.lineEdit_username.text()
                password = dlg.lineEdit_pwd.text()
                homedir = str(pathlib.Path.home())
                dir_path = os.path.join(homedir, ".config")
                if not os.path.exists(dir_path):
                    os.makedirs(dir_path)
                if url:
                    with open(os.path.join(dir_path, 'adminer.cfg'), 'w') as f:
                        f.write(f"url={url}")
                        f.write('\r\n')
                        f.write(f"username={username}")
                        f.write('\r\n')
                        f.write(f"password={password}")
                print("Save Proxy Config Success!")

    def chooseFile(self):
        homedir = str(pathlib.Path.home())

        filename_path, ok = QFileDialog.getOpenFileName(self, "Please Choose File", homedir,
                                                        "Xlsx Files (*.xlsx);;Xls Files (*.xls);;CSV Files (*.csv)")
        # print(filename_path)
        self.ui.lineEdit_filepath.setText(filename_path)
        self.ui.textEdit_input.setPlainText('')

    def loginFuc(self):
        # msgBox = QMessageBox(self)
        # msgBox.setWindowTitle("Demeter")
        self.ui.textBrowser.clear()
        language = 'en'  # self.buttonGroup.checkedButton().text()
        text_urls = self.ui.textEdit_input.toPlainText()
        url_list = set()
        if text_urls:
            _list = text_urls.split('\n')
            for item in _list:
                if item and str(item).startswith('http'):
                    url_list.add(item.strip())

        file_path = self.ui.lineEdit_filepath.text()
        if file_path:
            df = pd.read_excel(file_path, header=None)
            rows = df.values
            for p_idx, r in enumerate(rows):
                if r[0] and str(r[0]).startswith('http'):
                    url_list.add(str(r[0]).strip())

        variables = {}
        for p_idx, page_url in enumerate(url_list):
            if p_idx == 0:
                variables = parse(page_url)
        variables['url_list'] = url_list
        checked = self.ui.checkBox_proxy.checkState()
        variables['use_proxy'] = False
        if checked == Qt.Checked:
            variables['use_proxy'] = True

        variables['interval'] = self.ui.spinBox.text()
        variables['export_type'] = self.ui.comboBox_export.currentText()
        # variables['run_index'] = p_idx
        # variables['total_count'] = len(url_list)
        batch_no = parse_foodpandaV2(variables)
        all_log = self.ui.textBrowser.toPlainText()
        save_log(all_log, batch_no)

        # else:
        #     variables = parse(page_url)
        #     if variables == {}:
        #         # self.a = 'Unsupported page addresses'
        #         # self.ListWidget.addItem(self.a)
        #         print('Unsupported page addresses')
        #     else:
        #         self.pushButton.setDisabled(True)
        #         variables['language'] = language
        #         print('Language is ' + language)
        #         if variables.get('type') == 'foodgrab':
        #
        #             # self.a = parse_foodgrabV2(page_url, variables)
        #             # for i in self.a:
        #             #     self.ListWidget.addItem(i)
        #             # self.mainLayout.addWidget(self.ListWidget)
        #             # self.setLayout(self.mainLayout)
        #             try:
        #                 parse_foodgrabV2(page_url, variables)
        #                 print("Collection complete")
        #             except Exception as e:
        #                 print('Collection Fail!')
        #                 print("Collection Exception: %s" % e)
        #                 traceback.print_exc()
        #                 self.pushButton.setDisabled(False)
        #                 # msgBox.exec()
        #
        #         elif variables.get('type') == 'foodpanda':
        #
        #             # //self.a = parse_foodpanda(page_url, variables)
        #             # //for i in self.a:
        #             #     self.ListWidget.addItem(i)
        #             # self.mainLayout.addWidget(self.ListWidget)
        #             # self.setLayout(self.mainLayout)
        #             try:
        #                 parse_foodpandaV2(page_url, variables)
        #                 print("Collection complete")
        #             except Exception as e:
        #                 print('Collection Fail!')
        #                 print("Collection Exception: %s" % e)
        #                 traceback.print_exc()
        #                 self.pushButton.setDisabled(False)
        #                 # msgBox.exec()
        #         self.pushButton.setDisabled(False)

        #         print(variables)


class ProxyDlg(Ui_Dialog, QDialog):
    """Employee dialog."""

    def __init__(self, parent=None):
        super().__init__(parent)
        # Run the .setupUi() method to show the GUI
        self.setupUi(self)
        homedir = str(pathlib.Path.home())
        file_path = os.path.join(homedir, ".config", 'adminer.cfg')
        if os.path.isfile(file_path):
            f = open(file_path)
            lines = f.readlines()
            for line in lines:
                kv = line.split("=")
                if kv[0] == 'url':
                    self.lineEdit_url.setText(kv[1].strip())
                if kv[0] == 'username':
                    self.lineEdit_username.setText(kv[1].strip())
                if kv[0] == 'password':
                    self.lineEdit_pwd.setText(kv[1].strip())

            f.close()


if __name__ == "__main__":
    app = QApplication(sys.argv)
    widget = MainWindow()
    widget.show()
    sys.exit(app.exec())
