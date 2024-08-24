import os, re
from datetime import timedelta, datetime
from multiprocessing import Pipe, Process

from PyQt6 import QtGui
from PyQt6.QtCore import Qt, QEvent
from PyQt6.QtWidgets import QWidget, QListWidget, QGridLayout, \
    QLabel, QMenu, QMainWindow, QLayout, QListWidgetItem, \
    QAbstractItemView, QMessageBox, QLineEdit, QPushButton, QCheckBox, QVBoxLayout, QFrame, QHBoxLayout
from PyQt6.QtGui import QIcon, QAction

from controllers.app import AppController
from utilities import consts, utils
from views.custome import QClickableLabel


class LoginForm(QWidget):
    def __init__(self, state=None):
        super(LoginForm, self).__init__()
        self.__init_ui()
        self.screen = None
        layout = QGridLayout()

        label_name = QLabel('Email')
        label_name.setStyleSheet(consts.GENERAL_QLabel_STYLESHEET)
        self.lineEdit_username = QLineEdit()
        self.lineEdit_username.setStyleSheet(consts.GENERAL_QLineEdit_STYLESHEET)
        self.lineEdit_username.setPlaceholderText('Please enter your email...')
        layout.addWidget(label_name, 0, 0)
        layout.addWidget(self.lineEdit_username, 0, 1, 1, 3, )

        label_password = QLabel('Password')
        label_password.setStyleSheet(consts.GENERAL_QLabel_STYLESHEET)
        self.lineEdit_password = QLineEdit()

        self.lineEdit_password.setStyleSheet(consts.GENERAL_QLineEdit_STYLESHEET)
        self.lineEdit_password.setEchoMode(QLineEdit.EchoMode.Password)
        self.lineEdit_password.setPlaceholderText('Please enter your password...')

        self.__show_pass_action = QAction(QIcon(consts.UNHIDDEN_EYE_ICON_PATH), 'Show password', self)
        self.__show_pass_action.setCheckable(True)
        self.__show_pass_action.toggled.connect(self.show_password)
        self.lineEdit_password.addAction(self.__show_pass_action, QLineEdit.ActionPosition.TrailingPosition)

        layout.addWidget(label_password, 1, 0)
        layout.addWidget(self.lineEdit_password, 1, 1, 1, 3, )

        self.remember_me = QCheckBox('Remember me')
        self.remember_me.setStyleSheet(consts.SMALLER_QLabel_STYLESHEET)
        layout.addWidget(self.remember_me, 2, 0)

        label_forget_password = QClickableLabel('Forget Password?', self.forget_password, )
        label_forget_password.setStyleSheet(consts.SMALLER_QLabel_STYLESHEET)
        layout.addWidget(label_forget_password, 2, 3)

        button_login = QPushButton('Login')
        button_login.setStyleSheet(consts.GENERAL_QPushButton_STYLESHEET)
        button_login.clicked.connect(self.check_password)
        layout.addWidget(button_login, 3, 0, 1, 4, )

        layout.setRowMinimumHeight(2, 150)
        layout.setContentsMargins(15, 25, 15, 25)
        self.setLayout(layout)

        if state is None:
            # self.__try_remember_me_login()
            pass

    def show_password(self, ):
        if self.lineEdit_password.echoMode() == QLineEdit.EchoMode.Normal:
            self.lineEdit_password.setEchoMode(QLineEdit.EchoMode.Password)
            self.__show_pass_action.setIcon(QIcon(consts.UNHIDDEN_EYE_ICON_PATH))
        else:
            self.lineEdit_password.setEchoMode(QLineEdit.EchoMode.Normal)
            self.__show_pass_action.setIcon(QIcon(consts.HIDDEN_EYE_ICON_PATH))

    def __init_ui(self):
        self.setWindowTitle(consts.APP_NAME + ' -- Login Form')
        height = consts.LOGIN_SCREEN_HEIGHT
        width = consts.LOGIN_SCREEN_WIDTH
        self.resize(width, height)
        self.setMinimumHeight(height)
        self.setMaximumHeight(height)

        self.setMinimumWidth(width)
        self.setMaximumWidth(width)

        pass

    def check_password(self):

        msg = QMessageBox()
        email = self.lineEdit_username.text().lower()
        password = self.lineEdit_password.text()
        if email is None or not email:
            msg.setText('Please enter an email.')
            msg.exec()
            return

        if password is None or not password:
            msg.setText('Please enter a password.')
            msg.exec()
            return

        regex = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
        if not re.fullmatch(regex, email):
            msg.setText('Email must be in an email format.')
            msg.exec()
            return

        user = self.__load_user_data(email, password)

        # if self.remember_me.isChecked():
        #     utils.remember_me({
        #         'email': user.email,
        #         'password': user.password,
        #     },
        #         consts.REMEMBER_ME_FILE_PATH
        #     )
        #
        # print('done checking')
        if user:
            # self.next_screen(user)
            pass

    def forget_password(self, event):
        self.screen = ForgetPasswordForm()
        self.screen.show()
        self.hide()
        self.destroy()
        self.close()

    def __load_user_data(self, email, password):
        msg = QMessageBox()
        from models.models import User
        user = AppController.conn.query(User).filter(User.email == email).first()
        if user:
            if user.password == password:
                return user
            else:
                msg.setText("Password is not correct.")
                return False
        else:
            msg.setText("User is not found. Please sign up.")
            return False

    # def __try_remember_me_login(self, ):
    #     msg = QMessageBox()
    #     data = utils.get_me(consts.REMEMBER_ME_FILE_PATH)
    #
    #     if data:
    #         user = self.__load_user_data(
    #             email=data['email'],
    #             password=data['password'],
    #         )
    #     else:
    #         msg.setText("Could not load pre-saved user.")
    #         msg.exec()
    #         return

    # utils.remember_me({
    #     'email': user.email,
    #     'password': user.password,
    # }, consts.REMEMBER_ME_FILE_PATH)
    # self.next_screen(user)
    # return True

    def next_screen(self, user):
        self.screen = MainApp(
            user=user,
        )
        self.screen.show()

        self.hide()
        self.destroy()
        self.close()

        pass


class ForgetPasswordForm(QWidget):
    """ This "window" is a QWidget. If it has no parent, it will appear as a free-floating window as we want.
    """

    def __init__(self, ):
        super(ForgetPasswordForm, self).__init__()
        self.__init_ui()

        layout = QGridLayout()

        label_name = QLabel('Email')
        label_name.setStyleSheet(consts.GENERAL_QLabel_STYLESHEET)
        layout.addWidget(label_name, 0, 0)

        self.lineEdit_username = QLineEdit()
        self.lineEdit_username.setStyleSheet(consts.GENERAL_QLineEdit_STYLESHEET)
        self.lineEdit_username.setPlaceholderText('Please enter your email...')
        layout.addWidget(self.lineEdit_username, 0, 1, )

        button_check = QPushButton('Check')
        button_check.adjustSize()
        button_check.setStyleSheet(consts.GENERAL_QPushButton_STYLESHEET)

        button_check.clicked.connect(self.check_password)
        layout.addWidget(button_check, 1, 1, )

        button_back = QPushButton('Back')
        button_back.adjustSize()
        button_back.setStyleSheet(consts.GENERAL_QPushButton_STYLESHEET)
        button_back.clicked.connect(self.return_to_login_page)
        layout.addWidget(button_back, 1, 0, 1, 1)

        # layout.setRowMinimumHeight(10, 75)
        layout.setContentsMargins(10, 0, 10, 0)
        self.setLayout(layout)

    def __init_ui(self):
        self.setWindowTitle(consts.APP_NAME + ' -- Forget Password Form')
        height = consts.FORGET_PASSWORD_SCREEN_HEIGHT
        width = consts.FORGET_PASSWORD_WIDTH
        self.resize(width, height)
        self.setMinimumHeight(height)
        self.setMaximumHeight(height)

        self.setMinimumWidth(width)
        self.setMaximumWidth(width)

        pass

    def check_password(self):
        msg = QMessageBox()
        email = self.lineEdit_username.text().lower()
        if email is None or email == '':
            msg.setText('Please enter an email to validate.')
            msg.exec()
            return

        regex = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
        if not re.fullmatch(regex, email):
            msg.setText('Text must be in an email format.')
            msg.exec()
            return

        # res = MainController.API_CONNECTION.post_forget_password_request(email=email)
        # if res:
        #     msg.setText('You will receive an email with the details.')
        # else:
        #     msg.setText("Could not find this user in the system.")
        msg.exec()
        return

    def return_to_login_page(self):
        LoginForm(state='reverse').show()
        self.hide()
        self.destroy()
        self.close()
        pass


class MainApp(QMainWindow):

    def __init__(self, user=None):
        super(MainApp, self).__init__()

        self.__init_ui()

        self.user = user

    def __logout(self):

        if os.path.exists(consts.REMEMBER_ME_FILE_PATH):
            os.remove(consts.REMEMBER_ME_FILE_PATH)

        if os.path.exists(consts.REMEMBER_LAST_ACTIVE_FILE_PATH):
            os.remove(consts.REMEMBER_LAST_ACTIVE_FILE_PATH)

        LoginForm(state='reverse').show()
        self.hide()
        self.destroy()
        self.close()

    def __init_ui(self, ):
        self.setWindowTitle(consts.APP_NAME)
        width = int(consts.MAIN_SCREEN_HEIGHT * 1.61)
        self.setGeometry(
            consts.MAIN_SCREEN_LEFT_CORNER_START,
            consts.MAIN_SCREEN_TOP_CORNER_START,
            width,
            consts.MAIN_SCREEN_HEIGHT,
        )

        self.setMinimumHeight(consts.MAIN_SCREEN_HEIGHT)
        self.setMaximumHeight(consts.MAIN_SCREEN_HEIGHT)

        self.setMinimumWidth(width)
        self.setMaximumWidth(width)
        # self.setWindowIcon(QIcon('logo.png'))
