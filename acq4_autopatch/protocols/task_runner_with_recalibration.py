import sys
import time
import threading

import numpy as np
import pyqtgraph as pg

try:
    import queue
except ImportError:
    import Queue as queue
from acq4.Manager import getManager
from acq4.util import Qt
from acq4.util.threadrun import runInGuiThread
from .task_runner import TaskRunnerPatchProtocol


class TaskRunnerPatchRecalProtocol(TaskRunnerPatchProtocol):
    """Patch protocol implementing:

    - Move to cell
    - Recalibrate target
    - Recalibrate pipette
    - take brightfield photo, autopatch 
    - Initiate TaskRunner protocol
    - Clean pipette
    - Move pipette home and request swap (if broken / clogged)
    """

    name = "task runner with recalibration"

    def __init__(self, patchThread, patchAttempt):
        TaskRunnerPatchProtocol.__init__(self, patchThread, patchAttempt)
        self.dev = patchThread.dev
        self.module = patchThread.module
        self.clickEvent = threading.Event()
        self.stageCameraLock = self.module.stageCameraLock
        self.camera = self.module.getCameraDevice()
        self.cameraMod = self.module.getCameraModule()
        self.scope = self.camera.getScopeDevice()

        man = getManager()
        self.dh = man.getCurrentDir().mkdir(f"patch_attempt_{self.patchAttempt.pid:04d}", autoIncrement=True)
        patchAttempt.setLogFile(self.dh["patch.log"])

        self.stateQueue = queue.Queue()
        # this code is running in a thread, so it is necessary to specify that
        # the signal must be delivered in the main thread (since we are not running an event loop)
        self.dev.stateManager().sigStateChanged.connect(self.devStateChanged, Qt.Qt.DirectConnection)

    def runPatchProtocol(self):
        pa = self.patchAttempt
        

        if not self.dev.isTipClean():
            self.cleanPipette()

        try:
            self.dev.setState("bath")
            time.sleep(5)

            self.recalibratePipette()
            time.sleep(5)

            self.patchCell()

            finalState = self.dev.getState()
            if finalState.stateName != "whole cell":
                raise Exception(f"Failed to reach whole cell state (ended at {finalState}).")


            with self.stageCameraLock.acquire() as fut:
                pa.setStatus("Waiting for stage/camera")
                self.wait([fut], timeout=None)
                self.configureCamera()
                self.runProtocol(pa)

        except:
            pa.setError(sys.exc_info())
        finally:
            if self.dev.broken:
                self.swapPipette()
            elif not self.dev.clean:
                self.cleanPipette()

    def patchCell(self):
        # at this point, pipette should be 10um above the target cell
        pa = self.patchAttempt

        # Set target cell position, taking error correction into account
        targetPos = pa.pipetteTargetPosition()
        if not np.all(np.isfinite(targetPos)):
            raise Exception("No valid target position for this attempt (probably automatic recalibration failed)")

        pa.setStatus("moving to target")
        self.dev.pipetteDevice.setTarget(targetPos)
        self.clearStateQueue()

        # don't use target move here; we don't need all the obstacle avoidance.
        # kick off cell detection; wait until patched or failed
        pa.setStatus("cell patching")
        self.dev.setState("cell detect")
        while True:
            self.checkStop()
            try:
                state = self.stateQueue.get(timeout=0.2)
            except queue.Empty:
                continue

            if state.stateName in ("fouled", "broken"):
                return
            elif state.stateName in ("whole cell"):
                time.sleep(2)
                tph = self.dev.testPulseHistory()
                cp = tph['capacitance'][-100:].mean()
                ra = tph['peakResistance'][-100:].mean()
                self.dev.clampDevice.autoWholeCellCompensate(cp, ra) # Whole cell compensation (TBD)'
                time.sleep(2)
                return
            else:
                pa.setStatus(f"cell patching: {state.stateName}")

            while True:
                try:
                    # raise exception if this state fails
                    state.wait(timeout=0.2)
                    break
                except state.Timeout:
                    self.checkStop()

    def runProtocol(self, pa):
        """Cell is patched; lock the stage and begin protocol.
        """
        # focus camera on cell
        pa.setStatus("focus on cell")
        self.camera.moveCenterToGlobal(pa.globalTargetPosition(), speed="fast", center="roi").wait()

        man = getManager()
        turret = man.getDevice("FilterTurret")
        illum = man.getDevice("Illumination")

        # set filter wheel / illumination
        turret.setPosition(0).wait()
        time.sleep(2)  # scope automatically changes RL/TL settings, sometimes in a bad way. sleep and set manually:
        # illum.SetTLIllumination(1)
        # illum.SetRLIllumination(1)
        illum.setSourceActive('illum', 1) # Turn on brightfield

        # take a picture
        pa.setStatus("say cheese!")
        frame = self.camera.acquireFrames(n=1, stack=False)
        frame.saveImage(self.dh, "patch_image.tif")

        pa.setStatus("running whole cell protocol")

        # switch to RL
        turret.setPosition(0).wait()
        time.sleep(2)  # scope automatically changes RL/TL settings, sometimes in a bad way. sleep and set manually:
        illum.setSourceActive('illum', 0) # Turn off brightfield
        time.sleep(1)

        try: 
            self.camera.setParams({"exposure": 0.01, "binning": (4, 4)})
            cameraParams = self.camera.getParams()

            # frame = self.camera.acquireFrames(n=1, stack=False)
            # frame.saveImage(self.dh, "fluor_image.tif")

            man = getManager()
            # TODO: select correct task runner for this pipette
            taskrunner = None
            for mod in man.listModules():
                if not mod.startswith("Task Runner"):
                    continue
                mod = man.getModule(mod)
                if self.dev.clampDevice.name() in mod.docks:
                    taskrunner = mod
                    break

            assert taskrunner is not None, f"No task runner found that uses {self.dev.clampDevice.name()}"

            # 500Hz
            self.camera.setParams(
                {
                    "regionH": 164,
                    "regionY": 940,
                    "regionX": 8,
                    "regionW": 2032,
                    "exposure": 0.002,
                    "binning": (4, 4),
                }
            )

            # prepare camera to be triggered by the DAQ for this pipette
            self.configureCamera()
            fut = runInGuiThread(taskrunner.runSequence, store=True, storeDirHandle=self.dh)
            try:
                self.wait([fut], timeout=300)
            except self.patchThread.Stopped:
                fut.stop()
                raise

        finally:
            # Turn off whole cell compensation
            self.dev.clampDevice.mc.setParam('WholeCellCompEnable', 0)
            # switch off RL
            turret.setPosition(0).wait()
            time.sleep(10)  # scope automatically changes RL/TL settings, sometimes in a bad way. sleep and set manually:
            # illum.SetTLIllumination(1)
            # illum.SetRLIllumination(1)
            illum.setSourceActive('illum', 1) # Turn on brightfield
            self.camera.setParams(cameraParams)  # , autoRestart=True, autoCorrect=True)
            time.sleep(5) # force pulse for 2nd otherwise camera might error out
            pa.setStatus("restart acquire video of camera")
            self.camera.start()

        time.sleep(2)
        pa.setStatus("whole cell protocol complete")

        """Cell is patched; lock the stage and begin protocol.
        """
        # focus camera on cell
        # avoid this movement in 2P. In the future, add autofocus for 1P at this step
        pa.setStatus("focus on cell")       
        # self.camera.moveCenterToGlobal(pa.globalTargetPosition(), speed="fast", center="roi").wait()

        man = getManager()
        turret = man.getDevice("FilterTurret")
        illum = man.getDevice("Illumination")

        # set filter wheel / illumination
        turret.setPosition(1) #.wait() wait seems to be causing problem
        time.sleep(2)  # scope automatically changes RL/TL settings, sometimes in a bad way. sleep and set manually:
        illum.setSourceActive('illum', 1) # Turn on brightfield

        # take a picture
        pa.setStatus("say cheese!")
        frame = self.camera.acquireFrames(n=1, stack=False)
        frame.saveImage(self.dh, "patch_image.tif")

        pa.setStatus("running whole cell protocol")

        # turn off bright field
        illum.setSourceActive('illum', 0) # Turn off brightfield
        time.sleep(1)

        try: 
            self.camera.setParams({"exposure": 0.01, "binning": (4, 4)})
            cameraParams = self.camera.getParams()

            man = getManager()
            # TODO: select correct task runner for this pipette
            taskrunner = None
            for mod in man.listModules():
                if not mod.startswith("Task Runner"):
                    continue
                mod = man.getModule(mod)
                if self.dev.clampDevice.name() in mod.docks:
                    taskrunner = mod
                    break

            assert taskrunner is not None, f"No task runner found that uses {self.dev.clampDevice.name()}"
            
            # Adjust focus for 2P imaging
            pos = self.camera.globalCenterPosition() + np.array([0, 0, 7.690e-6])
            fut = self.camera.moveCenterToGlobal(pos, 'slow') # Slowly defocus
            self.wait([fut])

            # 300 Hz
            # self.camera.setParams({'regionH': 700, 'regionY': 680, 'regionX': 8, 'regionW': 2028, 'exposure': 0.0030013})
            # 500 Hz
            self.camera.setParams(
                {
                    "regionH": 164,
                    "regionY": 940,
                    "regionX": 8,
                    "regionW": 2032,
                    "exposure": 0.002,
                    "binning": (4, 4),
                }
            )

            # prepare camera to be triggered by the DAQ for this pipette
            self.configureCamera()
            fut = runInGuiThread(taskrunner.runSequence, store=True, storeDirHandle=self.dh)
            try:
                self.wait([fut], timeout=300)
            except self.patchThread.Stopped:
                fut.stop()
                raise

        finally:
            # Turn off whole cell compensation
            # skip temporarily
            # self.dev.clampDevice.mc.setParam('WholeCellCompEnable', 0)

            # Switch to GFP filter
            turret.setPosition(0) #.wait()
            time.sleep(2)  # scope automatically changes RL/TL settings, sometimes in a bad way. sleep and set manually:

            # Adjust the focus for 1P imaging
            pos = self.camera.globalCenterPosition() + np.array([0, 0, -7.690e-6])
            fut = self.camera.moveCenterToGlobal(pos, 'slow') # Slowly defocus
            self.wait([fut])

            # capture a fluorescence image for record (at this point we don't care about photobleaching)
            self.camera.setParams(cameraParams)  # default is full FOV
            LED = man.getDevice("LDILaser470") 
            LED.setChanHolding('TTL', 1) 
            frame = self.camera.acquireFrames(n=1, stack=False)
            frame.saveImage(self.dh, "fluor_image.tif")
            LED.setChanHolding('TTL', 0) 

            # switch off RL
            turret.setPosition(1) # .wait()
            time.sleep(2)  # scope automatically changes RL/TL settings, sometimes in a bad way. sleep and set manually:
            illum.setSourceActive('illum', 1) # Turn on brightfield            
            time.sleep(5) # force pause for 2nd otherwise camera might error out

            pa.setStatus("restart acquire video of camera")
            self.camera.start()

        time.sleep(2)
        pa.setStatus("whole cell protocol complete")

# The following methods are adapted from recalibrate.py
        
    def recalibratePipette(self):
        # How far above target to run calibration?
        #  - low values (10 um) potentially have worse machine vision performance due to being very close to cells
        #  - high values (100 um) potentially yield a bad correction due to errors that accumulate over the large distance to the cell
        calibrationHeight = 30e-6

        pa = self.patchAttempt
        if not hasattr(pa, "originalPosition"):
            pa.originalPosition = np.array(pa.position)

        # move to 100 um above current position
        pos = self.dev.pipetteDevice.globalPosition()
        pos[2] += 100e-6
        fut = self.dev.pipetteDevice._moveToGlobal(pos, "fast")
        self.wait([fut])

        # move to 100 um above target z value
        pos = pa.pipetteTargetPosition()
        pos[2] += 100e-6
        fut = self.dev.pipetteDevice._moveToGlobal(pos, "fast")
        self.wait([fut])

        # set pipette target position
        self.dev.pipetteDevice.setTarget(pa.pipetteTargetPosition())

        # move pipette to 10 um above corrected target
        pipPos = pa.pipetteTargetPosition() + np.array([0, 0, calibrationHeight])
        # don't use target move here; we don't need all the obstacle avoidance.
        # fut = self.dev.pipetteDevice.goTarget(speed='fast')
        pfut = self.dev.pipetteDevice._moveToGlobal(pipPos, speed="slow")

        with self.stageCameraLock.acquire() as fut:
            pa.setStatus("Waiting for stage/camera")
            self.wait([fut], timeout=None)

            # move stage/focus above actual target
            camPos = pa.globalTargetPosition() + np.array([0, 0, calibrationHeight])
            cfut = self.camera.moveCenterToGlobal(camPos, "fast")
            self.wait([pfut, cfut], timeout=None)

            # Offset from target to where pipette actually landed
            try:
                self.patchAttempt.pipetteError = self.getPipetteError()
            except RuntimeError:
                self.patchAttempt.pipetteError = np.array([np.nan] * 3)
                raise

    def getPipetteError(self):
        """Return error vector that should be added to pipette position fotr the current target.

        Error vector may contain NaN to indicate that the correction failed and this target should not be attempted.
        """
        pa = self.patchAttempt
        pa.setStatus("Measuring pipette error")

        perfVals = []
        pipetteDiffVals = []
        targetErrVals = []
        focusErrVals = []

        targetPos = np.array(pa.pipetteTargetPosition())

        # Make a few attempts to optimize pipette position. Iterate until
        #  - z is in focus on the pipette tip
        #  - pipette x,y is over the target
        for i in range(4):
            cameraPos = self.camera.globalCenterPosition("roi")

            # pipette position according to manipulator
            reportedPos = np.array(self.dev.pipetteDevice.globalPosition())

            # estimate tip position measured by machine vision
            measuredPos, perf = self.dev.pipetteDevice.tracker.measureTipPosition(threshold=0.4, movePipette=False)
            measuredPos = np.array(measuredPos)

            # generate some error metrics:
            # how far is the pipette from its reported position
            pipetteDiff = measuredPos - reportedPos
            # how far in Z is the pipette from the focal plane
            focusError = abs(measuredPos[2] - cameraPos[2])
            # how far in XY is the pipette from the target
            targetDiff = targetPos[:2] - measuredPos[:2]
            targetError = np.linalg.norm(targetDiff)

            # track performance so we can decide later whether to abandon this point
            perfVals.append(perf)
            pipetteDiffVals.append(pipetteDiff)
            focusErrVals.append(focusError)
            targetErrVals.append(targetError)

            # show the error line and pause briefly (just for debugging; we could remove this to speed up the process)
            self.showErrorLine(reportedPos, measuredPos)

            futs = []
            if focusError > 3e-6:
                # refocus on pipette tip (don't move pipette in z because if error prediction is wrong, we could crash)
                cameraPos[2] = measuredPos[2]
                futs.append(self.camera.moveCenterToGlobal(cameraPos, "slow"))

            if targetError > 1.5e-6:
                # reposition pipette x,y closer to target
                ppos = reportedPos.copy()
                ppos[:2] += targetDiff
                futs.append(self.dev.pipetteDevice._moveToGlobal(ppos, "slow"))

            if len(futs) > 0:
                # wait for requested moves to complete and try again
                self.wait(futs)
                time.sleep(0.3)  # wait for positions to catch up.. we can remove this after bug fixed!
                pa.setStatus(f"Measuring pipette error: adjust and iterate  ({i:d})")
            else:
                # no moves needed this round; we are done.
                break

        # Now decide whether to pass or fail this calibration.
        if focusErrVals[-1] > 3e-6 or targetErrVals[-1] > 3e-6 or perfVals[-1] < 0.5:
            raise RuntimeError(
                f"Measuring pipette error: failed  (focus error: {focusErrVals}  target error: {targetErrVals}  correlation: {perfVals})"
            )

        pa.setStatus(f"Measuring pipette error: success {pipetteDiff}")
        return pipetteDiff

    def showErrorLine(self, pt1, pt2):
        runInGuiThread(self._showErrorLine, pt1, pt2)
        time.sleep(1.5)
        runInGuiThread(self._removeErrorLine)

    def _showErrorLine(self, pt1, pt2):
        self._removeErrorLine()
        self.line = Qt.QGraphicsLineItem(pt1[0], pt1[1], pt2[0], pt2[1])
        self.line.setPen(pg.mkPen("r"))
        self.cameraMod.window().addItem(self.line)

    def _removeErrorLine(self):
        if self.line is None:
            return
        self.line.scene().removeItem(self.line)
        self.line = None