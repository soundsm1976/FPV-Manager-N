#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FPV-Race Manager — Live-таймер + конфиг порогов (2025-07-13)
Исправления: сумісність з macOS/Windows, обробка USB/Wi-Fi, фільтрація пакетів, точна реєстрація часу в RaceTab, додана функція додавання учасників.
"""

# ─────────── imports ───────────
import csv
import json
import socket
import sys
import threading
import time
import queue
from pathlib import Path
from typing import Optional
import serial
import serial.tools.list_ports
from PySide6.QtCore import Qt, QTimer, QMetaObject, Slot, Signal
from PySide6.QtWidgets import (
    QApplication, QWidget, QTabWidget, QVBoxLayout, QLabel, QTableWidget,
    QTableWidgetItem, QPushButton, QFileDialog, QHBoxLayout, QMessageBox,
    QHeaderView, QAbstractItemView, QComboBox, QLineEdit)

# ─────────── logging setup ───────────
import logging
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# ─────────── runtime-config ───────────
SERIAL_PORT: Optional[str] = None  # None → авто-пошук
SERIAL_BAUD = 115_200
SERIAL_ENCODING = "ascii"
TCP_HOST = ""  # "" для відключення TCP
TCP_PORT = 4000
UDP_LISTEN = False
UDP_PORT = 12345
MIN_THRESHOLD_DIFF = 10
MV_MIN = 0
MV_MAX = 5000
GATE_MIN = 1
GATE_MAX = 8
MAX_PACKETS_PER_CYCLE = 10
app_is_running = True

# ─────────── queues and locks ───────────
q_race = queue.Queue(1000)
q_settings = queue.Queue(200)
serial_tx = queue.Queue(200)
data_lock = threading.Lock()
queue_lock = threading.Lock()

# ─────────── TCP Connection Manager ───────────
class TCPConnection:
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.sock = None
        self.lock = threading.Lock()
        self.connected = False

    def connect(self):
        with self.lock:
            if self.sock is None:
                try:
                    self.sock = socket.create_connection((self.host, self.port), timeout=5)
                    self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                    self.connected = True
                    logger.info(f"Connected to {self.host}:{self.port}")
                except (ConnectionRefusedError, socket.timeout) as e:
                    self.connected = False
                    logger.error(f"Failed to connect to {self.host}:{self.port}: {e}")
                    self.sock = None

    def send(self, data: bytes) -> bool:
        with self.lock:
            if self.sock is None:
                self.connect()
            if self.sock is None:
                return False
            try:
                self.sock.sendall(data)
                logger.debug(f"Sent TCP data: {data}")
                return True
            except (BrokenPipeError, ConnectionResetError) as e:
                logger.error(f"TCP send error: {e}")
                self.connected = False
                self.sock = None
                return False

    def close(self):
        with self.lock:
            if self.sock:
                try:
                    self.sock.close()
                except Exception as e:
                    logger.error(f"TCP close error: {e}")
                self.sock = None
                self.connected = False

tcp_connection = TCPConnection(TCP_HOST, TCP_PORT)

def _auto_serial() -> Optional[str]:
    with data_lock:
        if SERIAL_PORT:
            return SERIAL_PORT
    ports = serial.tools.list_ports.comports()
    for port in ports:
        if "usb" in port.device.lower() or "com" in port.device.lower() or "tty" in port.device.lower() or "acm" in port.device.lower() or "wch" in port.device.lower():
            logger.info(f"Found serial port: {port.device}")
            return port.device
    logger.warning("No suitable serial port found")
    return None

def _serial_worker():
    retry_delay = 2
    while app_is_running:
        port = _auto_serial()
        if not port:
            logger.warning("No serial port found, retrying...")
            time.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, 30)
            continue
        try:
            with serial.Serial(port, SERIAL_BAUD, timeout=0.1) as ser:
                logger.info(f"Opened serial port: {port}")
                buf = b""
                while app_is_running:
                    try:
                        with queue_lock:
                            while not serial_tx.empty():
                                ser.write(serial_tx.get_nowait())
                        chunk = ser.read(256)
                        if chunk:
                            buf += chunk
                            while b"\n" in buf:
                                line, buf = buf.split(b"\n", 1)
                                if line.strip():
                                    if line.startswith(b"Received command:") or line.startswith(b"#"):
                                        logger.debug(f"Ignoring non-JSON packet: {line.strip()}")
                                        continue
                                    try:
                                        pkt = json.loads(line.strip().decode(SERIAL_ENCODING, errors="replace"))
                                        target_queue = q_settings if "thrOn" in pkt else q_race
                                        queue_name = "q_settings" if target_queue == q_settings else "q_race"
                                        with queue_lock:
                                            try:
                                                target_queue.put_nowait(line.strip())
                                            except queue.Full:
                                                logger.warning(f"{queue_name} full, dropping oldest packet")
                                                target_queue.get_nowait()
                                                target_queue.put_nowait(line.strip())
                                    except json.JSONDecodeError as e:
                                        logger.error(f"Serial JSON parse error: {e}, raw: {line.strip()}")
                    except serial.SerialException as e:
                        logger.error(f"Serial error: {e}")
                        break
        except serial.SerialException as e:
            logger.error(f"Serial setup error: {e}")
            time.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, 30)

def _tcp_reader(tcp_host: str):
    if not tcp_host or not tcp_host.strip():
        logger.warning("No TCP host specified, skipping")
        return
    retry_delay = 2
    while app_is_running:
        try:
            with socket.create_connection((tcp_host, TCP_PORT), timeout=5) as s:
                s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                logger.info(f"Connected to {tcp_host}:{TCP_PORT}")
                retry_delay = 2
                buf = b""
                while app_is_running:
                    chunk = s.recv(256)
                    if not chunk:
                        raise ConnectionResetError("Connection closed by remote host")
                    buf += chunk
                    while b"\n" in buf:
                        line, buf = buf.split(b"\n", 1)
                        if line.strip():
                            if line.startswith(b"Received command:") or line.startswith(b"#"):
                                logger.debug(f"Ignoring non-JSON packet: {line.strip()}")
                                continue
                            try:
                                pkt = json.loads(line.strip().decode("utf-8", errors="replace"))
                                target_queue = q_settings if "thrOn" in pkt else q_race
                                queue_name = "q_settings" if target_queue == q_settings else "q_race"
                                with queue_lock:
                                    try:
                                        target_queue.put_nowait(line.strip())
                                    except queue.Full:
                                        logger.warning(f"{queue_name} full, dropping oldest packet")
                                        target_queue.get_nowait()
                                        target_queue.put_nowait(line.strip())
                            except json.JSONDecodeError as e:
                                logger.error(f"TCP JSON parse error: {e}, raw: {line.strip()}")
        except (ConnectionRefusedError, socket.timeout, ConnectionResetError) as e:
            logger.error(f"Error connecting to {tcp_host}:{TCP_PORT}: {e}")
            retry_delay = min(retry_delay * 2, 30)
            time.sleep(retry_delay)
        except Exception as e:
            logger.exception(f"Unexpected error in TCP reader: {e}")
            time.sleep(retry_delay)

def _udp_reader():
    if not UDP_LISTEN:
        logger.warning("UDP listening disabled")
        return
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.settimeout(1.0)
            s.bind(("", UDP_PORT))
            logger.info(f"Listening on UDP port {UDP_PORT}")
            while app_is_running:
                try:
                    data, _ = s.recvfrom(256)
                    if data.strip():
                        line = data.strip()
                        if line.startswith(b"Received command:") or line.startswith(b"#"):
                            logger.debug(f"Ignoring non-JSON packet: {line}")
                            continue
                        try:
                            pkt = json.loads(line.decode("utf-8", errors="replace"))
                            target_queue = q_settings if "thrOn" in pkt else q_race
                            queue_name = "q_settings" if target_queue == q_settings else "q_race"
                            with queue_lock:
                                try:
                                    target_queue.put_nowait(line)
                                except queue.Full:
                                    logger.warning(f"{queue_name} full, dropping oldest packet")
                                    target_queue.get_nowait()
                                    target_queue.put_nowait(line)
                        except json.JSONDecodeError as e:
                            logger.error(f"UDP JSON parse error: {e}, raw: {line}")
                except socket.timeout:
                    continue
    except Exception as e:
        logger.exception(f"Unexpected error in UDP reader: {e}")

def send_raw(data: bytes, tcp_host: str = TCP_HOST) -> bool:
    if not app_is_running:
        logger.debug("Skipping send: application closed")
        return False
    if not isinstance(data, bytes):
        data = str(data).encode("utf-8")
    if tcp_host and tcp_host.strip():
        if tcp_connection.send(data):
            return True
    with queue_lock:
        try:
            serial_tx.put_nowait(data)
            logger.debug(f"Sent via Serial: {data}")
            return True
        except queue.Full:
            logger.warning("Serial TX queue full")
            return False

# ═════════ GUI – Tabs ═════════
# ───── PilotTab ─────
class PilotTab(QWidget):
    pilots_updated = Signal()

    def __init__(self):
        super().__init__()
        lay = QVBoxLayout(self)
        self.tbl = QTableWidget(100, 1, self)
        self.tbl.setHorizontalHeaderLabels(["Ім'я"])
        self.tbl.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.tbl.setAlternatingRowColors(True)
        lay.addWidget(self.tbl)

        row = QHBoxLayout()
        row.addWidget(QPushButton("💾 Сохранить CSV", clicked=self.save_csv))
        row.addWidget(QPushButton("📂 Загрузить CSV", clicked=self.load_csv))
        row.addWidget(QPushButton("🗑 Очистить", clicked=self.clear_table))
        row.addWidget(QPushButton("➕ Добавить пилота", clicked=self.add_pilot))
        row.addStretch()
        lay.addLayout(row)

    def current_list(self) -> list[str]:
        with data_lock:
            return [
                self.tbl.item(r, 0).text().strip()
                for r in range(self.tbl.rowCount())
                if self.tbl.item(r, 0) and self.tbl.item(r, 0).text().strip()
            ]

    def add_pilot(self):
        if not app_is_running:
            logger.debug("Skipping add pilot: application closed")
            return
        try:
            acquired = data_lock.acquire(timeout=1.0)
            if not acquired:
                logger.error("Failed to acquire data_lock in add_pilot")
                QMessageBox.critical(self, "Ошибка", "Не удалось добавить пилота: блокировка данных")
                return
            logger.debug("Acquired data_lock in add_pilot")
            for r in range(self.tbl.rowCount()):
                item = self.tbl.item(r, 0)
                if not item or not item.text().strip():
                    self.tbl.setItem(r, 0, QTableWidgetItem(""))
                    self.tbl.setCurrentCell(r, 0)
                    logger.debug(f"Added pilot at row {r}")
                    self.tbl.viewport().update()  # Force GUI refresh
                    self.pilots_updated.emit()
                    return
            if self.tbl.rowCount() < 100:
                self.tbl.setRowCount(self.tbl.rowCount() + 1)
                self.tbl.setItem(self.tbl.rowCount() - 1, 0, QTableWidgetItem(""))
                self.tbl.setCurrentCell(self.tbl.rowCount() - 1, 0)
                logger.debug(f"Expanded table and added pilot at row {self.tbl.rowCount() - 1}")
                self.tbl.viewport().update()  # Force GUI refresh
                self.pilots_updated.emit()
        except Exception as e:
            logger.error(f"Error in add_pilot: {e}")
        finally:
            if data_lock.locked():
                data_lock.release()
                logger.debug("Released data_lock in add_pilot")

    def clear_table(self):
        if not app_is_running:
            logger.debug("Skipping clear table: application closed")
            return
        reply = QMessageBox.question(
            self, "Подтверждение", "Очистить список пилотов?",
            QMessageBox.Yes | QMessageBox.No
        )
        if reply == QMessageBox.Yes:
            with data_lock:
                self.tbl.clearContents()
                self.pilots_updated.emit()

    def save_csv(self):
        if not app_is_running:
            logger.debug("Skipping save CSV: application closed")
            return
        pilots = self.current_list()
        if not pilots:
            QMessageBox.warning(self, "Ошибка", "Нет пилотов для сохранения")
            return
        fn, _ = QFileDialog.getSaveFileName(
            self, "Сохранить список пилотов", str(Path.home() / "pilots.csv"), "CSV (*.csv)"
        )
        if fn:
            try:
                with open(fn, "w", newline="", encoding="utf-8") as f:
                    writer = csv.writer(f)
                    writer.writerow(["Имя пилота"])
                    for name in pilots:
                        writer.writerow([name])
                QMessageBox.information(self, "Успех", f"Сохранено {len(pilots)} пилотов")
            except OSError as e:
                logger.error(f"Failed to save CSV: {e}")
                QMessageBox.critical(self, "Ошибка", f"Не удалось сохранить файл: {e}")

    def load_csv(self):
        if not app_is_running:
            logger.debug("Skipping load CSV: application closed")
            return
        fn, _ = QFileDialog.getOpenFileName(
            self, "Загрузить список пилотов", str(Path.home()), "CSV (*.csv)"
        )
        if fn:
            try:
                with open(fn, encoding="utf-8") as f:
                    reader = csv.reader(f)
                    rows = list(reader)
                    self.tbl.setRowCount(100)
                    if rows and rows[0][0].lower() in ["имя", "name", "pilot", "имя пилота"]:
                        pilots = [row[0].strip() for row in rows[1:] if row and row[0].strip()]
                    else:
                        pilots = [row[0].strip() for row in rows if row and row[0].strip()]
                    for i, name in enumerate(pilots[:100]):
                        self.tbl.setItem(i, 0, QTableWidgetItem(name))
                    self.tbl.viewport().update()  # Force GUI refresh
                    self.pilots_updated.emit()
            except (OSError, IndexError) as e:
                logger.error(f"Failed to load CSV: {e}")
                QMessageBox.critical(self, "Ошибка", f"Не удалось загрузить файл: {e}")

# ───── SettingsTab ─────
class SettingsTab(QWidget):
    update_table_signal = Signal()

    def __init__(self, tx_func):
        super().__init__()
        self._tx = tx_func
        self.thresholds = [[780, 790] for _ in range(GATE_MAX)]
        self.detect_mode = [True] * GATE_MAX
        self.gate_order = list(range(GATE_MAX))
        self._setup_ui()
        self.timer = QTimer(self)
        self.timer.timeout.connect(self.process_packets)
        self.update_table_signal.connect(self._update_table)

    def _setup_ui(self):
        lay = QVBoxLayout(self)
        lay.addWidget(QLabel("Настройки ворот и подключения"))

        self.tbl = QTableWidget(GATE_MAX, 4)
        self.tbl.setHorizontalHeaderLabels(["Ворота", "Порог ВКЛ (mV)", "Порог ВЫКЛ (mV)", "Детекция"])
        self.tbl.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.tbl.setAlternatingRowColors(True)
        lay.addWidget(self.tbl)

        row = QHBoxLayout()
        row.addWidget(QPushButton("📤 Применить строку", clicked=self._apply_row))
        row.addWidget(QPushButton("🔄 Обновить все", clicked=self._refresh_all))
        row.addStretch()
        lay.addLayout(row)

        self._update_table()

    def _apply_row(self):
        r = self.tbl.currentRow()
        if r < 0:
            return
        try:
            on = int(self.tbl.item(r, 1).text())
            off = int(self.tbl.item(r, 2).text())
            if on < MV_MIN or off < MV_MIN or on > MV_MAX or off > MV_MAX:
                raise ValueError("Пороги вне диапазона")
            if off <= on + MIN_THRESHOLD_DIFF:
                raise ValueError("Недопустимая разница порогов")
            physical_gate = self.gate_order[r]
            self.thresholds[physical_gate] = [on, off]
            cmd = {"cfg": "thr", "gate": physical_gate + 1, "on": on, "off": off}
            self._tx(json.dumps(cmd).encode() + b"\n", TCP_HOST)
            self.update_table_signal.emit()
        except ValueError as e:
            QMessageBox.warning(self, "Ошибка", str(e))

    def _refresh_all(self):
        self._tx(b'{"cfg":"get"}\n', TCP_HOST)
        self.update_table_signal.emit()

    def _update_table(self):
        with data_lock:
            for row in range(GATE_MAX):
                physical_gate = self.gate_order[row]
                self.tbl.setItem(row, 0, QTableWidgetItem(f"G{physical_gate + 1}"))
                self.tbl.item(row, 0).setFlags(Qt.ItemIsEnabled | Qt.ItemIsSelectable)
                on_thr, off_thr = self.thresholds[physical_gate]
                self.tbl.setItem(row, 1, QTableWidgetItem(str(on_thr)))
                self.tbl.setItem(row, 2, QTableWidgetItem(str(off_thr)))
                detect_status = "ВКЛ" if self.detect_mode[physical_gate] else "ВЫКЛ"
                detect_item = QTableWidgetItem(detect_status)
                detect_item.setFlags(Qt.ItemIsEnabled | Qt.ItemIsSelectable)
                detect_item.setBackground(Qt.green if self.detect_mode[physical_gate] else Qt.red)
                self.tbl.setItem(row, 3, detect_item)

    def process_packets(self):
        if self.parent().currentWidget() != self:
            return
        packets = []
        with queue_lock:
            for _ in range(MAX_PACKETS_PER_CYCLE):
                try:
                    packets.append(q_settings.get_nowait())
                except queue.Empty:
                    break
        for raw in packets:
            try:
                pkt = json.loads(raw.decode("utf-8", errors="replace"))
                if "thrOn" in pkt and "thrOff" in pkt:
                    gate_num = pkt.get("gate", 0)
                    if GATE_MIN <= gate_num <= GATE_MAX:
                        self.update_threshold_from_device(gate_num, pkt["thrOn"], pkt["thrOff"])
            except json.JSONDecodeError as e:
                logger.error(f"Settings JSON parse error: {e}, raw: {raw}")

    def update_threshold_from_device(self, gate_num: int, on_thr, off_thr):
        if not (GATE_MIN <= gate_num <= GATE_MAX):
            logger.warning(f"Invalid gate number: {gate_num}")
            return
        try:
            on_thr = int(float(on_thr))
            off_thr = int(float(off_thr))
            if on_thr < MV_MIN or off_thr < MV_MIN or on_thr > MV_MAX or off_thr > MV_MAX:
                logger.warning(f"Thresholds out of range for gate {gate_num}: on={on_thr}, off={off_thr}")
                return
            if off_thr <= on_thr + MIN_THRESHOLD_DIFF:
                logger.warning(f"Invalid threshold diff for gate {gate_num}: on={on_thr}, off={off_thr}")
                return
            with data_lock:
                self.thresholds[gate_num - 1] = [on_thr, off_thr]
                self.update_table_signal.emit()
                logger.info(f"Updated gate {gate_num}: on={on_thr}, off={off_thr}")
        except (ValueError, TypeError) as e:
            logger.error(f"Error updating thresholds for gate {gate_num}: {e}")

# ───── RaceTab ─────
class RaceTab(QWidget):
    update_ui_signal = Signal()

    def __init__(self, pilot_tab: PilotTab, settings_tab: SettingsTab, tx_func):
        super().__init__()
        self.tx = tx_func
        self.pilot_tab = pilot_tab
        self.settings_tab = settings_tab
        self.active_pilot_idx = 0
        self.race_start_ts = None
        self.previous_ts = None
        self.total_race_time = 0.0
        self.gate_states = {i + 1: "below" for i in range(GATE_MAX)}
        self.previous_mv_values = {}
        self.finished_pilots = set()
        self.disqualified_pilots = set()
        self.gate_last_seen = [time.perf_counter_ns() // 1_000_000] * GATE_MAX
        self.finish_gate = GATE_MAX
        self.min_signal_change = 100
        self.signal_timeout = 5000
        self.last_seq = {}
        self._pending_ui_updates = False
        self._setup_ui()
        self.timer = QTimer(self)
        self.timer.timeout.connect(self.process_packets)
        self.update_ui_signal.connect(self._update_ui)
        self.pilot_tab.pilots_updated.connect(self.refresh_pilots)

    def _setup_ui(self):
        layout = QVBoxLayout(self)

        indicators_layout = QHBoxLayout()
        indicators_layout.addWidget(QLabel("Состояние ворот:"))
        self.gate_indicators = []
        for i in range(GATE_MAX):
            indicator = QLabel(f"G{i + 1} 🔴")
            indicator.setAlignment(Qt.AlignCenter)
            indicator.setStyleSheet("QLabel { border: 1px solid gray; padding: 5px; }")
            indicators_layout.addWidget(indicator)
            self.gate_indicators.append(indicator)
        indicators_layout.addStretch()
        layout.addLayout(indicators_layout)

        columns = ["Пилот"] + [f"G{i}" for i in range(GATE_MIN, GATE_MAX + 1)] + ["Общее время"]
        self.results_table = QTableWidget(0, len(columns))
        self.results_table.setHorizontalHeaderLabels(columns)
        self.results_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.results_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.results_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.results_table.setAlternatingRowColors(True)
        layout.addWidget(self.results_table)

        pilot_controls = QHBoxLayout()
        pilot_controls.addWidget(QPushButton("⬅ Предыдущий", clicked=self.prevPilot))
        pilot_controls.addWidget(QPushButton("➡ Следующий", clicked=self.nextPilot))
        pilot_controls.addWidget(QPushButton("DQ", clicked=self.toggle_dq))
        pilot_controls.addWidget(QPushButton("🏁 Фініш", clicked=self.finish_manual))
        pilot_controls.addStretch()
        layout.addLayout(pilot_controls)

        controls_layout = QHBoxLayout()
        controls_layout.addWidget(QLabel("Финишные ворота:"))
        self.gate_selector = QComboBox()
        self.gate_selector.addItems([f"G{i}" for i in range(GATE_MIN, GATE_MAX + 1)])
        self.gate_selector.setCurrentIndex(GATE_MAX - GATE_MIN)
        self.gate_selector.currentIndexChanged.connect(self._update_finish_gate)
        controls_layout.addWidget(self.gate_selector)
        controls_layout.addWidget(QPushButton("🏆 Подиум", clicked=self.show_winners))
        controls_layout.addWidget(QPushButton("🔄 Очистить", clicked=self.clear_table))
        controls_layout.addStretch()
        layout.addLayout(controls_layout)

        self.info_label = QLabel()
        layout.addWidget(self.info_label)
        self.refresh_pilots()

    @Slot(int)
    def _update_finish_gate(self, index: int):
        new_finish_gate = index + GATE_MIN
        if GATE_MIN <= new_finish_gate <= GATE_MAX:
            with data_lock:
                self.finish_gate = new_finish_gate
            logger.info(f"Finish gate set to G{self.finish_gate}")
        else:
            logger.warning(f"Invalid finish gate index: {index}, resetting to G{GATE_MAX}")
            with data_lock:
                self.finish_gate = GATE_MAX
            self.gate_selector.setCurrentIndex(GATE_MAX - GATE_MIN)

    @Slot()
    def refresh_pilots(self):
        names = self.pilot_tab.current_list()
        with data_lock:
            self.results_table.setUpdatesEnabled(False)
            self.results_table.setRowCount(len(names))
            for i, name in enumerate(names):
                self.results_table.setItem(i, 0, QTableWidgetItem(name))
            self.results_table.setUpdatesEnabled(True)
            self._pending_ui_updates = True
            self.update_ui_signal.emit()

    @Slot()
    def _update_ui(self):
        if not self._pending_ui_updates:
            return
        with data_lock:
            self._pending_ui_updates = False
            self._update_info()
            self._highlight_active_pilot()
            now = time.perf_counter_ns() // 1_000_000
            for g in range(GATE_MAX):
                alive = now - self.gate_last_seen[g] < 2000
                txt = self.gate_indicators[g].text()
                self.gate_indicators[g].setText(txt.replace("🔴", "🟢") if alive else txt.replace("🟢", "🔴"))
            if self.results_table.rowCount():
                self.results_table.scrollToItem(self.results_table.item(self.active_pilot_idx, 0))

    def _update_info(self):
        names = self.pilot_tab.current_list()
        with data_lock:
            current_pilot = names[self.active_pilot_idx] if self.active_pilot_idx < len(names) else "—"
        self.info_label.setText(f"Участников: {len(names)}   •   Активный: {current_pilot}")

    def _highlight_active_pilot(self):
        with data_lock:
            for r in range(self.results_table.rowCount()):
                bg = Qt.yellow if r == self.active_pilot_idx else (Qt.red if r in self.disqualified_pilots else Qt.white)
                for c in range(self.results_table.columnCount()):
                    item = self.results_table.item(r, c)
                    if not item:
                        item = QTableWidgetItem("")
                        self.results_table.setItem(r, c, item)
                    item.setBackground(bg)

    def prevPilot(self):
        self._select_pilot(max(self.active_pilot_idx - 1, 0))

    def nextPilot(self):
        with data_lock:
            if self.race_start_ts is not None and self.active_pilot_idx not in self.finished_pilots:
                reply = QMessageBox.question(
                    self, "Подтверждение", "Переключиться на следующего пилота? Текущая гонка будет сброшена.",
                    QMessageBox.Yes | QMessageBox.No
                )
                if reply != QMessageBox.Yes:
                    return
            self._select_pilot(min(self.active_pilot_idx + 1, self.results_table.rowCount() - 1))

    def _select_pilot(self, idx):
        with data_lock:
            self.active_pilot_idx = idx
            self.race_start_ts = None
            self.previous_ts = None
            self.total_race_time = 0.0
            self.gate_states = {i + 1: "below" for i in range(GATE_MAX)}
            self.previous_mv_values = {}
            self.last_seq = {}
            self._pending_ui_updates = True
            self.update_ui_signal.emit()

    def toggle_dq(self):
        with data_lock:
            if self.active_pilot_idx in self.disqualified_pilots:
                self.disqualified_pilots.remove(self.active_pilot_idx)
            else:
                self.disqualified_pilots.add(self.active_pilot_idx)
            self._pending_ui_updates = True
            self.update_ui_signal.emit()

    def finish_manual(self):
        with data_lock:
            if self.race_start_ts is None:
                QMessageBox.warning(self, "Ошибка", "Гонка не начата")
                return
            total_time = self.total_race_time or ((time.perf_counter_ns() // 1_000_000) - self.race_start_ts) / 1000.0
            self.results_table.setItem(self.active_pilot_idx, GATE_MAX + 1, QTableWidgetItem(f"{total_time:.3f}"))
            self.finished_pilots.add(self.active_pilot_idx)
            self._pending_ui_updates = True
            self.update_ui_signal.emit()
        logger.info(f"Manual finish for pilot {self.active_pilot_idx}: {total_time:.3f}")
        QMetaObject.invokeMethod(self, "nextPilot", Qt.QueuedConnection)

    def clear_table(self):
        reply = QMessageBox.question(
            self, "Подтверждение", "Очистить таблицу результатов?",
            QMessageBox.Yes | QMessageBox.No
        )
        if reply == QMessageBox.Yes:
            with data_lock:
                for r in range(self.results_table.rowCount()):
                    for c in range(1, self.results_table.columnCount()):
                        self.results_table.setItem(r, c, QTableWidgetItem(""))
                self.finished_pilots.clear()
                self.disqualified_pilots.clear()
                self._select_pilot(self.active_pilot_idx)
                self._pending_ui_updates = True
                self.update_ui_signal.emit()
            logger.info("Results table cleared")

    def show_winners(self):
        results = []
        with data_lock:
            for r in self.finished_pilots:
                if r not in self.disqualified_pilots:
                    item = self.results_table.item(r, GATE_MAX + 1)
                    if item and item.text():
                        try:
                            results.append((float(item.text()), self.results_table.item(r, 0).text()))
                        except ValueError:
                            continue
        if not results:
            QMessageBox.information(self, "Подиум", "Пока нет финишей")
            return
        results.sort()
        text = "\n".join(f"{i + 1}. {name} — {time:.3f} с" for i, (time, name) in enumerate(results))
        QMessageBox.information(self, "🏆 Лидеры", text)

    def process_packets(self):
        if self.parent().currentWidget() != self:
            return
        packets = []
        with queue_lock:
            for _ in range(MAX_PACKETS_PER_CYCLE):
                try:
                    packets.append(q_race.get_nowait())
                except queue.Empty:
                    break
        updated = False
        now = time.perf_counter_ns() // 1_000_000
        for raw in packets:
            try:
                pkt = json.loads(raw.decode("utf-8", errors="replace"))
                logger.debug(f"Parsed race packet: {pkt}")
                gate = pkt.get("gate", 0)
                if not (GATE_MIN <= gate <= GATE_MAX):
                    logger.warning(f"Invalid gate: {gate}")
                    continue
                mv = pkt.get("mv", 0)
                ts = pkt.get("ts", 0)
                seq = pkt.get("seq", 0)
                if mv < MV_MIN or mv > MV_MAX:
                    logger.warning(f"Ignoring packet with invalid mv value: {mv} for gate {gate}")
                    continue
                if ts <= 0 or seq <= 0:
                    logger.warning(f"Ignoring packet with invalid ts: {ts} or seq: {seq} for gate {gate}")
                    continue
                with data_lock:
                    if not self.settings_tab.detect_mode[gate - 1]:
                        logger.debug(f"Ignoring gate {gate} due to detect mode OFF")
                        continue
                    # if gate in self.last_seq and seq <= self.last_seq[gate]:
                    #     logger.debug(f"Ignoring duplicate or old seq: {seq} for gate {gate}")
                    #     continue
                    self.last_seq[gate] = seq
                    self.gate_last_seen[gate - 1] = now
                    on_threshold, off_threshold = self.settings_tab.thresholds[gate - 1]
                    prev_mv = self.previous_mv_values.get(gate, MV_MIN)
                    prev_state = self.gate_states.get(gate, "below")
                    detected = False
                    if prev_state == "below" and mv >= on_threshold:
                        self.gate_states[gate] = "above"
                        logger.debug(f"Gate {gate} state changed to above, mv={mv}")
                    elif prev_state == "above" and mv < off_threshold and abs(mv - prev_mv) >= self.min_signal_change:
                        self.gate_states[gate] = "below"
                        detected = True
                        logger.info(f"Gate {gate} passage detected, mv={mv}, prev_mv={prev_mv}")
                    self.previous_mv_values[gate] = mv
                    if now - self.gate_last_seen[gate - 1] > self.signal_timeout:
                        self.gate_states[gate] = "below"
                        logger.debug(f"Gate {gate} state reset to below due to timeout")
                    if self.active_pilot_idx >= self.results_table.rowCount() or self.active_pilot_idx in self.disqualified_pilots:
                        logger.debug(f"Ignoring packet: invalid pilot index {self.active_pilot_idx} or disqualified")
                        continue
                    if detected and gate == GATE_MIN and self.race_start_ts is None:
                        self.race_start_ts = self.previous_ts = ts
                        self.results_table.setItem(self.active_pilot_idx, 1, QTableWidgetItem("0.000"))
                        logger.info(f"Started race for pilot {self.active_pilot_idx} at ts={ts}")
                        updated = True
                    elif detected and self.race_start_ts is not None:
                        total_time = (ts - self.race_start_ts) / 1000.0
                        if gate >= GATE_MIN + 1:
                            split_time = (ts - self.previous_ts) / 1000.0
                            self.results_table.setItem(self.active_pilot_idx, gate, QTableWidgetItem(f"{split_time:.3f}"))
                            logger.info(f"Gate {gate} time: {split_time:.3f} for pilot {self.active_pilot_idx}")
                        self.results_table.setItem(self.active_pilot_idx, GATE_MAX + 1, QTableWidgetItem(f"{total_time:.3f}"))
                        self.total_race_time = total_time
                        self.previous_ts = ts
                        if gate == self.finish_gate:
                            self.finished_pilots.add(self.active_pilot_idx)
                            logger.info(f"Finished pilot {self.active_pilot_idx}: {total_time:.3f}")
                            QMetaObject.invokeMethod(self, "nextPilot", Qt.QueuedConnection)
                        updated = True
            except json.JSONDecodeError as e:
                logger.error(f"Race JSON parse error: {e}, raw: {raw}")
        if updated:
            with data_lock:
                self._pending_ui_updates = True
            self.update_ui_signal.emit()

# ═════════ Main Window ═════════
class MainWin(QTabWidget):
    def __init__(self):
        super().__init__()
        self.pilot_tab = PilotTab()
        self.settings_tab = SettingsTab(send_raw)
        self.race_tab = RaceTab(self.pilot_tab, self.settings_tab, send_raw)
        self.addTab(self.race_tab, "Live")
        self.addTab(self.pilot_tab, "Участники")
        self.addTab(self.settings_tab, "Настройки")
        self.currentChanged.connect(self.on_tab_changed)

    def on_tab_changed(self, index):
        current_tab = self.widget(index)
        for i in range(self.count()):
            tab = self.widget(i)
            if isinstance(tab, (RaceTab, SettingsTab)) and tab != current_tab and tab.timer.isActive():
                tab.timer.stop()
                logger.info(f"Stopped timer for {tab.__class__.__name__}")
        if isinstance(current_tab, RaceTab) and not current_tab.timer.isActive():
            current_tab.timer.start(50)
            logger.info("Started RaceTab timer")
        elif isinstance(current_tab, SettingsTab) and not current_tab.timer.isActive():
            current_tab.timer.start(100)
            logger.info("Started SettingsTab timer")

    def closeEvent(self, event):
        global app_is_running
        app_is_running = False
        logger.info("Application closing, stopping all timers and threads")
        self.race_tab.timer.stop()
        self.settings_tab.timer.stop()
        tcp_connection.close()
        event.accept()

# ═════════ Entry Point ═════════
if __name__ == "__main__":
    threading.Thread(target=_serial_worker, daemon=True).start()
    threading.Thread(target=_tcp_reader, args=(TCP_HOST,), daemon=True).start()
    threading.Thread(target=_udp_reader, daemon=True).start()

    app = QApplication(sys.argv)
    win = MainWin()
    win.resize(1000, 650)
    win.show()
    sys.exit(app.exec())
