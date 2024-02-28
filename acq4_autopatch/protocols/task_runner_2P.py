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
from .task_runner import TaskRunnerPatchProtocol


class TaskRunner2PPatchProtocol(TaskRunnerPatchProtocol):
    """2P Patch protocol implementing:

    - Move to cell (taking into account 1P-2P offset), take brightfield photo
    - autopatch 
    - switch to 2P light path (control the turret and illumination. mSwitcher?)
    - Initiate TaskRunner protocol (send TTL signal to start recording in SlideBook)
    - Clean pipette
    - Move pipette home and request swap (if broken / clogged)

      Most functions are inherited from TaskRunnerPatchProtocol, except for the runProtocol which added extra steps for 2P imaging.
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
            # 1kHz
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