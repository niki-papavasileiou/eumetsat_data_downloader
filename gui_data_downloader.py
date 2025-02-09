import logging
import sys
import threading
import warnings

from PyQt5.QtCore import QDate, Qt
from PyQt5.QtGui import QTextCursor
from PyQt5.QtWidgets import (
    QApplication,
    QComboBox,
    QDateEdit,
    QFileDialog,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)
from functions.downloaders import download_col_realtime, download_col_custom

warnings.filterwarnings("ignore")
warnings.simplefilter(action="ignore", category=RuntimeWarning)


class QTextEditLogger(logging.Handler):
    def __init__(self, textbox):
        super().__init__()
        self.textbox = textbox

    def emit(self, record):
        msg = self.format(record)
        self.textbox.moveCursor(QTextCursor.End)
        self.textbox.insertPlainText(msg + "\n")
        self.textbox.moveCursor(QTextCursor.End)
        QApplication.processEvents()


class DownloaderApp(QWidget):
    def __init__(self):
        super().__init__()

        self.downloading = False  # Flag to indicate if downloading is in progress
        self.collected_data_thread = None  # Store the collected data thread
        self.selected_folder = ""

        self.init_ui()
        self.setup_logging()

    def init_ui(self):
        self.setWindowTitle("Data Downloader")

        # Title label
        title_label = QLabel("Data Downloader")
        title_label.setAlignment(Qt.AlignCenter)
        title_label.setStyleSheet("font-size: 20px; font-weight: bold;")

        self.btn_select_folder = QPushButton("Select Folder")
        self.btn_select_folder.clicked.connect(self.select_folder)

        # Textbox
        self.textbox = QTextEdit()
        self.textbox.setReadOnly(True)

        # Download mode selection
        self.mode_dropdown = QComboBox()
        self.mode_dropdown.addItems(["Real-time", "Custom"])
        self.mode_dropdown.currentIndexChanged.connect(self.update_controls)

        # Collection selection dropdown
        self.collection_dropdown = QComboBox()
        self.collection_dropdown.addItems(
            ["firerisk", "cloud_mask", "SEVIRI", "IASI", "FCI", "FCI_LI"]
        )
        self.collection_dropdown.setEnabled(False)

        # Date selection
        self.from_date_edit = QDateEdit()
        self.from_date_edit.setCalendarPopup(True)
        self.from_date_edit.setDate(
            QDate.currentDate().addDays(-7)
        )  # Default from date

        self.to_date_edit = QDateEdit()
        self.to_date_edit.setCalendarPopup(True)
        self.to_date_edit.setDate(QDate.currentDate())  # Default to date

        # Apply style sheet to date selection widgets
        self.apply_calendar_style(self.from_date_edit)
        self.apply_calendar_style(self.to_date_edit)

        # Buttons
        self.btn_download = QPushButton("Download Data")
        self.btn_download.clicked.connect(self.download_data)

        self.btn_stop = QPushButton("Stop Download")
        self.btn_stop.clicked.connect(self.stop_download)
        self.btn_stop.setEnabled(False)  # Initially disabled until download starts

        # Buttons layout
        buttons_layout = QHBoxLayout()
        buttons_layout.addWidget(self.btn_download)
        buttons_layout.addWidget(self.btn_stop)
        buttons_layout.setSpacing(10)
        buttons_layout.setAlignment(Qt.AlignCenter)

        # Main layout
        layout = QVBoxLayout()
        layout.addWidget(title_label)
        layout.addWidget(QLabel("Select Download Folderpath:"))
        layout.addWidget(self.btn_select_folder)

        layout.addWidget(QLabel("Select Download Mode:"))
        layout.addWidget(self.mode_dropdown)
        layout.addWidget(QLabel("Select Collection:"))
        layout.addWidget(self.collection_dropdown)
        layout.addWidget(QLabel("Select Date Range:"))
        layout.addWidget(self.from_date_edit)
        layout.addWidget(self.to_date_edit)
        layout.addWidget(self.textbox)
        layout.addLayout(buttons_layout)
        layout.setContentsMargins(20, 20, 20, 20)
        self.setLayout(layout)

        # Set main window style
        self.setStyleSheet(
            """
            QWidget {
                background-color: #f2f2f2;
            }
            QPushButton {
                background-color: #5190e4;
                border: none;
                color: white;
                padding: 10px 20px;
                text-align: center;
                text-decoration: none;
                font-size: 16px;
                margin: 4px 2px;
                border-radius: 8px;
            }
            QPushButton:hover {
                background-color: #003276;
            }
            """
        )

    def select_folder(self):
        # Open a dialog to select the folder
        folder = QFileDialog.getExistingDirectory(self, "Select Folder")
        if folder:
            self.selected_folder = folder
            self.logger.info(f"Selected folder: {self.selected_folder}")

    def apply_calendar_style(self, date_edit):
        # Customize the QDateEdit's calendar popup
        calendar_widget = date_edit.calendarWidget()

        # Set background and text color for the calendar
        calendar_style = (
            "QCalendarWidget QAbstractItemView {"
            "background-color: white; "
            "color: black; "
            "}"
            "QCalendarWidget QWidget {"
            "color: black; "
            "}"
            "QCalendarWidget QToolButton {"
            "color: black; "
            "}"
        )
        calendar_widget.setStyleSheet(calendar_style)

    def setup_logging(self):
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.INFO)

        # Overwrite the log file each time the application runs
        file_handler = logging.FileHandler("app.log", mode="w")
        file_handler.setLevel(logging.INFO)
        file_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        file_handler.setFormatter(file_formatter)

        gui_handler = QTextEditLogger(self.textbox)
        gui_handler.setLevel(logging.INFO)
        gui_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        gui_handler.setFormatter(gui_formatter)

        self.logger.addHandler(file_handler)
        self.logger.addHandler(gui_handler)

    def update_controls(self, index):
        if index == 0:  # Real-time mode selected
            self.collection_dropdown.setEnabled(False)
            self.from_date_edit.setEnabled(False)
            self.to_date_edit.setEnabled(False)
        elif index == 1:  # Custom mode selected
            self.collection_dropdown.setEnabled(True)
            self.from_date_edit.setEnabled(True)
            self.to_date_edit.setEnabled(True)

    def download_data(self):
        mode_index = self.mode_dropdown.currentIndex()

        if mode_index == 0:  # Real-time mode
            self.start_realtime_download()
        elif mode_index == 1:  # Custom mode
            self.start_custom_download()

    def start_realtime_download(self):
        if not self.downloading:  # Only start a new download if not already downloading
            self.downloading = True
            self.btn_stop.setEnabled(True)
            self.btn_download.setEnabled(False)
            self.collected_data_thread = threading.Thread(
                target=download_col_realtime, args=(self.logger, self.downloading, self.selected_folder
)
            )
            self.collected_data_thread.start()

    def start_custom_download(self):
        if not self.downloading:  # Only start a new download if not already downloading
            self.downloading = True
            self.btn_stop.setEnabled(True)
            self.btn_download.setEnabled(False)

            selected_collection = self.collection_dropdown.currentText()
            from_date = self.from_date_edit.date().toString("yyyy-MM-dd")
            to_date = self.to_date_edit.date().toString("yyyy-MM-dd")

            self.collected_data_thread = threading.Thread(
                target=download_col_custom,
                args=(
                    selected_collection,
                    from_date,
                    to_date,
                    self.btn_stop,
                    self.logger,
                    self.downloading,
                    self.btn_download,
                ),
            )
            self.collected_data_thread.start()

    def stop_download(self):
        self.downloading = False
        if self.collected_data_thread and self.collected_data_thread.is_alive():
            self.collected_data_thread.join()  # Wait for the thread to finish
        self.btn_stop.setEnabled(False)
        self.btn_download.setEnabled(True)


if __name__ == "__main__":
    app = QApplication(sys.argv)
    downloader = DownloaderApp()
    downloader.show()
    sys.exit(app.exec_())
