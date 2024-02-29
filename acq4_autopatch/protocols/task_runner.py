import sys
import time

import numpy as np

try:
    import queue
except ImportError:
    import Queue as queue
from acq4.Manager import getManager
from acq4.util import Qt
from acq4.util.threadrun import runInGuiThread
from .patch_protocol import PatchProtocol


class TaskRunnerPatchProtocol(PatchProtocol):
    """Patch protocol implementing:

    - Move to cell, take brightfield photo, autopatch 
    - Initiate TaskRunner protocol
    - Clean pipette
    - Move pipette home and request swap (if broken / clogged)
    """

    name = "task runner"

    def __init__(self, patchThread, patchAttempt):
        PatchProtocol.__init__(self, patchThread, patchAttempt)
        self.dev = patchThread.dev
        self.module = patchThread.module
        self.stageCameraLock = self.module.stageCameraLock
        self.camera = self.module.getCameraDevice()
        self.scope = self.camera.getScopeDevice()

        man = getManager()
        self.dh = man.getCurrentDir().mkdir(f"patch_attempt_{self.patchAttempt.pid:04d}", autoIncrement=True)
        patchAttempt.setLogFile(self.dh["patch.log"])

        self.stateQueue = queue.Queue()
        # this code is running in a thread, so it is necessary to specify that
        # the signal must be delivered in the main thread (since we are not running an event loop)
        self.dev.stateManager().sigStateChanged.connect(self.devStateChanged, Qt.Qt.DirectConnection)

    def devStateChanged(self, stateManager, state):
        self.stateQueue.put(state)

    def runPatchProtocol(self):
        pa = self.patchAttempt

        if not self.dev.isTipClean():
            self.cleanPipette()

        try:
            self.dev.setState("bath")
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
        pa = self.patchAttempt

        # Set target cell position, taking error correction into account
        targetPos = pa.pipetteTargetPosition()
        if not np.all(np.isfinite(targetPos)):
            raise Exception("No valid target position for this attempt (probably automatic recalibration failed)")

        pa.setStatus("moving to target")
        self.dev.pipetteDevice.setTarget(targetPos)

        # move to 100 um above cell, fast
        pos = np.array(targetPos) + np.array([100e-6, 100e-6, 100e-6])
        fut = self.dev.pipetteDevice._moveToGlobal(pos, speed="fast")
        self.wait([fut])

        # move to 10 um above cell, slow
        # pos = np.array(targetPos) + np.array([0, 0, 10e-6])
        # don't use target move here; we don't need all the obstacle avoidance.
        # fut = self.dev.pipetteDevice.goTarget(speed='fast')
        # fut = self.dev.pipetteDevice._moveToGlobal(pos, speed="slow")
        fut = self.dev.pipetteDevice.goApproachTarget(speed='slow')
        self.wait([fut])

        self.clearStateQueue()

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

    def abortPatchProtocol(self):
        pass

    def clearStateQueue(self):
        # clear out information about any pipette states before now
        while not self.stateQueue.empty():
            self.stateQueue.get(timeout=0)

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
        # illum.SetTLIllumination(2)
        # illum.SetRLIllumination(2)
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

            # 300 Hz
            # self.camera.setParams({'regionH': 700, 'regionY': 680, 'regionX': 8, 'regionW': 2028, 'exposure': 0.0030013})
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

    def configureCamera(self):
        """Set camera exposure/trigger channels for this pipette's DAQ.
        """
        # note: we'd love it if the camera and DAQ could just automatically decide which trigger
        # channels to use, but that's not supported yet so this is a temporary workaround.
        if "cameraChannels" in self.module.config:
            exp, trig = self.module.config["cameraChannels"][self.dev.name()]
            self.camera.reconfigureChannel("exposure", {"channel": exp})
            self.camera.reconfigureChannel("trigger", {"channel": trig})

    def cleanPipette(self):
        pa = self.patchAttempt
        fut = self.dev.pipetteDevice.goHome('fast')
        self.wait([fut])
        pa.setStatus("cleaning pipette")
        self.clearStateQueue()     
        fut = self.dev.setState("clean")

        # wait for cleaning to finish
        self.wait([fut], timeout=120)

    def swapPipette(self):
        pa = self.patchAttempt
        pa.setStatus("requesting new pipette")
        self.dev.setState("out")
        self.dev.goHome("fast")
        self.dev.requestNewPipette()

class TaskRunner2PPatchProtocol(TaskRunnerPatchProtocol):
    """2P Patch protocol implementing:

    - Move to cell (taking into account 1P-2P offset), take brightfield photo
    - autopatch 
    - switch to 2P light path (control the turret and illumination. mSwitcher?)
    - Initiate TaskRunner protocol (send TTL signal to start recording in SlideBook)
    - Clean pipette
    - Move pipette home and request swap (if broken / clogged)

      Most functions are inherited from TaskRunnerPatchProtocol
    """

    name = "task runner 2P"

    def __init__(self, patchThread, patchAttempt):
        TaskRunnerPatchProtocol.__init__(self, patchThread, patchAttempt)
        self.dev = patchThread.dev
        self.module = patchThread.module
        self.stageCameraLock = self.module.stageCameraLock
        self.camera = self.module.getCameraDevice()
        self.scope = self.camera.getScopeDevice()

        man = getManager()
        self.dh = man.getCurrentDir().mkdir(f"patch_attempt_{self.patchAttempt.pid:04d}", autoIncrement=True)
        patchAttempt.setLogFile(self.dh["patch.log"])

        self.stateQueue = queue.Queue()
        # this code is running in a thread, so it is necessary to specify that
        # the signal must be delivered in the main thread (since we are not running an event loop)
        self.dev.stateManager().sigStateChanged.connect(self.devStateChanged, Qt.Qt.DirectConnection)

    def patchCell(self):
        pa = self.patchAttempt

        # Set target cell position, taking error correction into account
        targetPos = pa.pipetteTargetPosition()
        if not np.all(np.isfinite(targetPos)):
            raise Exception("No valid target position for this attempt (probably automatic recalibration failed)")

        pa.setStatus("moving to target")
        self.dev.pipetteDevice.setTarget(targetPos)

        # move pipette to 100 um above cell, fast
        pos = np.array(targetPos) + np.array([100e-6, 100e-6, 100e-6])
        fut = self.dev.pipetteDevice._moveToGlobal(pos, speed="fast")
        self.wait([fut])

        # Move the stageXY to position the target in the center of 2P FOV
        pos = np.array(targetPos) + np.array([-12.43e-6, -152.9e-6, 0]) # TBD: move this offset to global configuration
        fut = self.camera.moveCenterToGlobal(pos, 'slow')
        self.wait([fut])

        # move pipette to 20 um above cell, slow
        pos = np.array(targetPos) + np.array([0, 0, 20e-6])
        # don't use target move here; we don't need all the obstacle avoidance.
        # fut = self.dev.pipetteDevice.goTarget(speed='fast')
        fut = self.dev.pipetteDevice._moveToGlobal(pos, speed="slow")
        self.wait([fut])

        self.clearStateQueue()

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

            frame = self.camera.acquireFrames(n=1, stack=False)
            frame.saveImage(self.dh, "fluor_image.tif")

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
            self.dev.clampDevice.mc.setParam('WholeCellCompEnable', 0)
            # switch off RL
            turret.setPosition(0).wait()
            time.sleep(10)  # scope automatically changes RL/TL settings, sometimes in a bad way. sleep and set manually:
            illum.setSourceActive('illum', 1) # Turn on brightfield
            self.camera.setParams(cameraParams)  # , autoRestart=True, autoCorrect=True)
            time.sleep(5) # force pulse for 2nd otherwise camera might error out
            pa.setStatus("restart acquire video of camera")
            self.camera.start()

        time.sleep(2)
        pa.setStatus("whole cell protocol complete")