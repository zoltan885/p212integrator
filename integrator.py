#!/home/hegedues/anaconda3/envs/pyfai/bin python3
# -*- coding: utf-8 -*-
"""
Created on Thu Sep 28 15:14:29 2023

@author: hegedues
"""

import sys
import os
import time
import logging
import glob
from watchdog.observers import Observer
from watchdog.events import LoggingEventHandler
from watchdog.events import FileSystemEventHandler
import watchdog.events
import watchdog.observers
import pyFAI, fabio
from pyFAI import AzimuthalIntegrator
import multiprocessing
from multiprocessing import Event, Process, Queue
import queue


from PyQt5 import QtWidgets, uic, QtGui
from PyQt5.QtCore import QRunnable, Qt, QThreadPool, pyqtSignal, QThread, QObject
from PyQt5.QtWidgets import (
    QMainWindow,
    QLabel,
    QGridLayout,
    QWidget,
    QPushButton,
    QProgressBar,
    QFileDialog,
    )



RAW_FOLDER = '/gpfs/current/raw/'
PROC_FOLDER = '/gpfs/current/processed/'
IMG_EXTS = ['cbf', 'tif']

logging.basicConfig(format='%(asctime)s %(message)s',
                    datefmt='%Y.%m.%d %I:%M:%S %p',
                    encoding='utf-8',
                    level=logging.DEBUG)


class WindowWidget(QtWidgets.QWidget):
    def __init__(self, *args, **kwargs):
        super(WindowWidget, self).__init__(*args, **kwargs)
        uic.loadUi('integrator.ui', self)
        self.RAWFolder = None
        self.PROCESSEDFolder = None
        self.PONI = None
        self.reevaluateFiles = []
        self.extension = IMG_EXTS[0]
        self._setupExtension()
        self.NIntProcs = 1
        self._setupNIntProcs()
        self._integrator = None

        # reevaluate
        self.reevRunning = False
        self._reevQueue = CustomQueue()
        self._reevaluateHandlerRunSignal = Event()

        # live
        self.liveRunning = False
        self.liveQueue = CustomQueue()
        self._liveHandlerRunSignal = Event()
        self._eventHandler = Handler(self.liveQueue, self._liveHandlerRunSignal, extension=self.extension)
        self.WD = WatchDog(self.RAWFolder, self._eventHandler)

        # pyFAI parameters
        self.pyFAIparameters = {'npoints': 2000, 'splitting': 'full_splitting', 'unit': 'q_A^-1'}
        self.pyFAIparametersValid = True


        self.pushButton_openRAW.clicked.connect(self.openFolderRAW)
        self.pushButton_openPROCESSED.clicked.connect(self.openFolderPROCESSED)
        self.pushButton_openPONI.clicked.connect(self.openPONI)
        self.comboBox_noProcesses.currentIndexChanged.connect(self._updateNIntProcs)
        self.comboBox_extension.currentIndexChanged.connect(self._updateExtension)
        self.pushButton_listFolderContent.clicked.connect(self._listFolderContent)
        self.pushButton_initializeIntegrator.clicked.connect(self._initializeIntegrator)
        self.toolButton_reevaluateRUN.clicked.connect(self.reevaluationRUN)
        self.toolButton_liveRUN.clicked.connect(self.liveRUN)


    def _setupNIntProcs(self):
        ncpu = multiprocessing.cpu_count()
        logging.debug(f"N CPU: {ncpu}")
        for i in range(2, max(2, ncpu-2)+1):
            self.comboBox_noProcesses.addItem(str(i))

    def _updateNIntProcs(self):
        self.NIntProcs = self.comboBox_noProcesses.currentIndex() + 1
        logging.debug(f"N procs to use: {self.NIntProcs}")

    def _setupExtension(self):
        for ext in IMG_EXTS:
            self.comboBox_extension.addItem(ext)
        self.comboBox_extension.setEnabled(True)

    def _updateExtension(self):
        self.extension = self.comboBox_extension.currentText()
        self.label_extension.setText(self.extension)
        logging.debug(f"Extension: {self.extension}")

    def openFolderRAW(self):
        self.RAWFolder = QFileDialog.getExistingDirectory(self, "Select a folder", RAW_FOLDER, QFileDialog.ShowDirsOnly)
        self.label_RAW.setText(self.RAWFolder)
        self.lineEdit_RAW.setText(self.RAWFolder)
        self.label_reevalCurrentFolder.setText(self.RAWFolder)

    def openFolderPROCESSED(self):
        self.PROCESSEDFolder = QFileDialog.getExistingDirectory(self, "Select a folder", PROC_FOLDER, QFileDialog.ShowDirsOnly)
        self.label_PROCESSED.setText(self.PROCESSEDFolder)
        self.lineEdit_PROCESSED.setText(self.PROCESSEDFolder)

    def openPONI(self):
        self.PONI = QFileDialog.getOpenFileName(self, "Select the PONI file", RAW_FOLDER, '*poni')[0]
        print(self.PONI)
        self.label_PONI.setText(self.PONI)
        self.lineEdit_PONI.setText(self.PONI)

    def _listFolderContent(self):  # should be done with a separate process
        if self.RAWFolder is None:
            print('Error: RAW folder not initialized yet.')
            return
        # clear content if it was initialized before
        self.textBrowser_reevaluate.clear()
        while not self._reevQueue.empty():
            _ = self._reevQueue.get()
        self.reevaluateFiles = listFolderContent(self.RAWFolder, self.extension)
        self.label_notEvaluatedFiles.setText('Not evaluated files found: %d' % len(self.reevaluateFiles))
        for i,f in enumerate(self.reevaluateFiles):
            self._reevQueue.put(f)
            self.textBrowser_reevaluate.append('%6d    %s' % (i, f))
        logging.debug(f"Re-evaluation queue size: {self._reevQueue.size}")

    def _initializeIntegrator(self):
        if self.RAWFolder is None:
            print('Error: RAW folder not initialized yet.')
            return
        if self.PROCESSEDFolder is None:
            print('Error: PROCESSED folder not initialized yet.')
            return
        if self.PONI is None:
            print('Error: poni file not initialized yet.')
            return
        try:
            self._integrator = _IntegratorApp(self.PONI, self.RAWFolder, self._liveHandlerRunSignal)
            print('Integrator initialized')
            self.label_statusLeft.setText('Integrator initialized')
        except Exception as e:
            print("ERROR: integrator initialization failed:\n%s" % e)
            return
        self.toolButton_reevaluateRUN.setEnabled(True)
        self.toolButton_liveRUN.setEnabled(True)

    def reevaluationRUN(self):
        self.reevRunning = self.toolButton_reevaluateRUN.isChecked()
        if self.reevRunning:
            self.toolButton_reevaluateRUN.setText('RUNNING...')
        else:
            self.toolButton_reevaluateRUN.setText('RUN')

    def liveRUN(self):
        # TODO start separate process to update queue size
        self.liveRunning = self.toolButton_liveRUN.isChecked()
        if self.liveRunning:
            self.toolButton_liveRUN.setText('RUNNING...')
            self._liveHandlerRunSignal.set()
            self.WD.run()
            qc = QueueChecker()
            QThreadPool.globalInstance().start(qc)

        else:
            self.toolButton_liveRUN.setText('RUN')
            self._liveHandlerRunSignal.clear()
            # empty the queue
            while not self.liveQueue.empty():
                _ = self.liveQueue.get()




class QueueChecker(QRunnable):
    def run(self):
        i = 0
        while True:
            print(i)
            i += 1
            time.sleep(1)




def listFolderContent(path, extension):
    content = glob.glob(os.path.join(path, '**/*.%s' % extension), recursive=True)
    content.sort(key=os.path.getmtime)
    return content




class CustomQueue():
    def __init__(self):
        self.Queue = queue.Queue()
        self._size = 0

    def put(self, *args, **kwargs):
        self.Queue.put(*args, **kwargs)
        self._size += 1

    def get(self, *args, **kwargs):
        item = self.Queue.get(*args, **kwargs)
        self._size -= 1
        return item

    @property
    def size(self):
        return self._size

    def empty(self):
        return True if self._size == 0 else False


def testCustomQueue():
    q = CustomQueue()
    print(q.size)
    for i in range(10):
        q.put(i)
    print(q.size)
    for i in range(5):
        print('\t', q.get())
    print(q.size)


class _IntegratorApp():
    def __init__(self, poni, rawfolder, runEvent):
        self.startEv, self.stopEv, self.pauseEv = Event(), Event(), Event()
        self.intWorkerBusyEv = Event()
        self.fileQueue = CustomQueue()
        self.ctrs = {'ims_in_queue': 0, 'processed_ims': 0}
        self.handler = Handler(self.fileQueue, runEvent)
        self.WD = WatchDog(rawfolder, self.handler)
        self.WDprocess = Process(target=self.WD.run, args=(self.stopEv, ))
        self.monitorProcess = Process(target=self._queue_monitor)
        self.ai = pyFAI.load(poni)

    def _queue_monitor(self):
        self.startEv.wait()
        ctr = 0
        while not self.stopEv.is_set():
            print(f"Queue size: {self.fileQueue.size}")
            ctr += 1
            if ctr %10 == 0:
                print('Printing queue content')
                while not self.fileQueue.empty():
                    print(self.fileQueue.get())
            time.sleep(1)

    def _integrate_worker(self, ai, fname, queue, npoint=2000, unit='q_A^-1'):
        logging.info(f'Working on {fname}')
        try:
            im = fabio.open(fname).data
        except IOError as e:
            logging.error(e)
        except:
            logging.error('Other error')
        #im -= 100
        folder = self._create_folder_for_file(fname)
        outfile = os.path.join(folder, fname.rpartition('/')[2].rpartition('.')[0]+'.dat')
        _ = ai.integrate1d(im, npoint, unit=unit, filename=outfile)
        logging.info(f"{fname} integration finished and saved as {outfile}")

    def _integrate_scheduler(self):
        while not self.stopEv.is_set():
            if self.pauseEv.is_set():
                time.sleep(0.1)
            else:
                while self.intWorkerBusyEv.is_set():
                    time.sleep(0.01)

    def _create_folder_for_file(self, fname):
        assert fname.startswith(RAW_FOLDER), f"{fname} is not in the raw folder"
        folder = os.path.join(PROC_FOLDER, fname.replace(RAW_FOLDER, '').rpartition('/')[0].strip('/'))
        if not os.path.exists(folder):
            os.makedirs(folder)
        return folder

    def run(self):
        self.monitorProcess.start()
        self.startEv.set()
        self.WDprocess.start()
        try:
            while True:
                time.sleep(0.1)
        except KeyboardInterrupt:
            self.stopEv.set()
            time.sleep(0.1)
            self.WDprocess.join()
            self.monitorProcess.join()






class IntegratorApp():
    def __init__(self):
        self.startEv, self.stopEv, self.pauseEv = Event(), Event(), Event()
        self.intWorkerBusyEv = Event()
        self.fileQueue = CustomQueue()
        self.ctrs = {'ims_in_queue': 0, 'processed_ims': 0}
        self.handler = Handler(self.fileQueue)
        self.WD = WatchDog('/home/hegedues/Documents/snippets/watchdog_integrator/watchdog_test/', self.handler)
        self.WDprocess = Process(target=self.WD.run, args=(self.stopEv, ))
        self.monitorProcess = Process(target=self._queue_monitor)
        self.ai = pyFAI.load('/home/hegedues/Documents/snippets/watchdog_integrator/CeO2.poni')

    def _queue_monitor(self):
        self.startEv.wait()
        ctr = 0
        while not self.stopEv.is_set():
            print(f"Queue size: {self.fileQueue.size}")
            ctr += 1
            if ctr %10 == 0:
                print('Printing queue content')
                while not self.fileQueue.empty():
                    print(self.fileQueue.get())
            time.sleep(1)

    def _integrate_worker(self, ai, fname, queue, npoint=2000, unit='q_A^-1'):
        logging.info(f'Working on {fname}')
        try:
            im = fabio.open(fname).data
        except IOError as e:
            logging.error(e)
        except:
            logging.error('Other error')
        #im -= 100
        folder = self._create_folder_for_file(fname)
        outfile = os.path.join(folder, fname.rpartition('/')[2].rpartition('.')[0]+'.dat')
        _ = ai.integrate1d(im, npoint, unit=unit, filename=outfile)
        logging.info(f"{fname} integration finished and saved as {outfile}")

    def _integrate_scheduler(self):
        while not self.stopEv.is_set():
            if self.pauseEv.is_set():
                time.sleep(0.1)
            else:
                while self.intWorkerBusyEv.is_set():
                    time.sleep(0.01)

    def _create_folder_for_file(self, fname):
        assert fname.startswith(RAW_FOLDER), f"{fname} is not in the raw folder"
        folder = os.path.join(PROC_FOLDER, fname.replace(RAW_FOLDER, '').rpartition('/')[0].strip('/'))
        if not os.path.exists(folder):
            os.makedirs(folder)
        return folder

    def run(self):
        self.monitorProcess.start()
        self.startEv.set()
        self.WDprocess.start()
        try:
            while True:
                time.sleep(0.1)
        except KeyboardInterrupt:
            self.stopEv.set()
            time.sleep(0.1)
            self.WDprocess.join()
            self.monitorProcess.join()






def _integrate_worker(ai, fname, outfile, queue, npoint=2000, unit='q_A^-1'):
    logging.info(f'Working on {fname}')
    try:
        im = fabio.open(fname).data
    except IOError as e:
        logging.error(e)
    except:
        logging.error('Other error')
    #im -= 100
    _ = ai.integrate1d(im, npoint, unit=unit, filename=outfile)
    logging.info(f'{fname} integration finished and saved as {outfile}')






class Handler(FileSystemEventHandler):
    def __init__(self, queue, run, extension='cbf'):
        self.queue = queue
        self.extension = extension
        self.run = run

    def on_any_event(self, event):
        if event.is_directory:
            return None
        elif event.event_type == 'created' and self.run.is_set():
            fname = event.src_path
            if self.extension in fname:
                while not fname.endswith(self.extension):
                    fname = fname[:-1]
            self.queue.put(fname)
            logging.info('created %s' % fname)
            print("Watchdog received created event - %s" % fname)


class WatchDog:
    def __init__(self, directory, handler):
        self.watchDirectory = directory
        self.handler = handler
        self.observer = Observer()

    def run(self, stopEv):
        self.observer.schedule(self.handler, self.watchDirectory, recursive=True)
        self.observer.start()
        while not stopEv.is_set():
            time.sleep(0.2)
        self.observer.stop()
        print("Observer Stopped")
        self.observer.join()


def mainGUI():
    app = QtWidgets.QApplication(sys.argv)
    main = WindowWidget()
    main.show()
    sys.exit(app.exec_())


if __name__ == "__main__":
    #I = IntegratorApp()
    #I.run()
    #testCustomQueue()
    mainGUI()

