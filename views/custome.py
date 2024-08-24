import logging

from datetime import timedelta, datetime
from typing import Union, Dict, Any, Optional

from PyQt6.QtCore import QTimer, Qt
from PyQt6.QtGui import QPixmap
from PyQt6.QtWidgets import QLabel, QWidget, QHBoxLayout, QStyledItemDelegate, QStyleOptionViewItem


class QClickableLabel(QLabel):
    def __init__(self, msg, when_clicked, parent=None):
        QLabel.__init__(
            self,
            msg,
            parent=parent
        )
        self._when_clicked = when_clicked

    def mouseReleaseEvent(self, event):
        self._when_clicked(event)


class ItemDelegate(QStyledItemDelegate):
    def paint(self, painter, option, index):
        option.decorationPosition = QStyleOptionViewItem.ViewItemPosition.Right
        super(ItemDelegate, self).paint(painter, option, index)