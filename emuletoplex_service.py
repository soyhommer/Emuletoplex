import win32serviceutil
import win32service
import win32event
import servicemanager
import subprocess
import sys
import os
from pathlib import Path

class EmuleToPlexService(win32serviceutil.ServiceFramework):
    _svc_name_ = "EmuleToPlex"
    _svc_display_name_ = "EmuleToPlex Folder Watcher"
    _svc_description_ = "Moves completed eMule downloads into Plex library structure and refreshes Plex."

    def __init__(self, args):
        win32serviceutil.ServiceFramework.__init__(self, args)
        self.hWaitStop = win32event.CreateEvent(None, 0, 0, None)
        self.process = None

    def SvcStop(self):
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        if self.process and self.process.poll() is None:
            self.process.terminate()
        win32event.SetEvent(self.hWaitStop)

    def SvcDoRun(self):
        servicemanager.LogInfoMsg("EmuleToPlex service starting")
        python = sys.executable
        script = str(Path(__file__).with_name("emuletoplex_runner.py"))
        self.process = subprocess.Popen([python, script], cwd=str(Path(script).parent))
        win32event.WaitForSingleObject(self.hWaitStop, win32event.INFINITE)
        servicemanager.LogInfoMsg("EmuleToPlex service stopped")

if __name__ == "__main__":
    win32serviceutil.HandleCommandLine(EmuleToPlexService)
