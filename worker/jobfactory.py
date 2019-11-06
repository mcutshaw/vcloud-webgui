import multiprocessing
from vcloud import vcloud
from db import vcloud_db
import datetime

class jobFactory:
    def __init__ (self, config, numProcs=8):
        self.config = config
        self.vcloud = vcloud.vcloud(config)
        self.catalog = self.vcloud.getCatalog(config['Extra']['Catalog'])
        self.vdc = self.vcloud.getVdc(config['Extra']['Vdc'])
        self.org = self.vcloud.getOrg(config['Main']['Org'])
        self.role = self.org.getRole(config['Deploy']['Role'])

        self.db = vcloud_db(config)

        self.numProcs = numProcs
        self.queue = multiprocessing.Queue()
        self.createProcs()


    def createProcs(self):
        for _ in range(self.numProcs):
            p = multiprocessing.Process(target=self._work, args=(self.config,))
            p.start()

    def _work(self, config):
        db = vcloud_db(config)
        while(True):
            task = self.queue.get()
            if task is None:
                print("Dying gracefully")
                break

            task_id = task[0]
            task_op = task[2]

            db.updateTaskStatus(task_id, "RUNNING")
            self.db.updateTaskStartedDate(task_id, datetime.datetime.now())

            if task_op == 'CAPTURE':
                self._tryAndLog(task, self._capture)
            elif task_op == 'UPDATEDB':
                self._tryAndLog(task, self._updateInfoTable)
            else:
                print("Unknown call", task[2])
                print("Discarding...", task[2])
                db.updateTaskStatus(task_id, "FAILED")
                db.updateTaskLog(task_id, "Unknown task type, discarded by main manager")

    def _capture(self, task):
        args = task[3].split(',')
        vapp_name = args[0]
        template_name = args[1]

        vapps = self.vcloud.getvApps(vapp_name)

        if vapps != []:
            vapp = vapps[0]
        else:
            self._failTask(task, f"No vapp with name {vapp_name}")
            return None

        print(vapp.status)
        if vapp.status == 'SUSPENDED':
            vapp.waitOnReady(timeout=300)
            vapp.unsuspend()
            vapp.waitOnReady(timeout=300)

        if vapp.status != 'POWERED_OFF':
            vapp.waitOnReady(timeout=300)
            vapp.powerOff()

        if not vapp.checkSnapshotExists():
            vapp.waitOnReady(timeout=300)
            vapp.snapshot()

        vapp.waitOnReady(timeout=300)
        vapp.capture(self.catalog,name=template_name)
        self._completeTask(task)

    def _updateInfoTable(self, task):
        pass

    def _tryAndLog(self, task, func):
        try:
            func(task)
        except Exception as e:
            self._failTask(task, e)

    def _failTask(self, task, reason):
        task_id = task[0]
        reason = str(reason).replace("'","\\'")
        self.db.updateTaskStatus(task_id, "FAILED")
        self.db.updateTaskLog(task_id, reason)
        self.db.updateTaskCompletedDate(task_id, datetime.datetime.now())

    def _completeTask(self, task, reason=None):
        task_id = task[0]
        self.db.updateTaskStatus(task_id, "COMPLETED")
        if reason is not None:
            reason = str(reason).replace("'","\\'")
            self.db.updateTaskLog(task_id, reason)
        self.db.updateTaskCompletedDate(task_id, datetime.datetime.now())
        
    def put(self, item):
        self.queue.put(item)

    def stop(self):
        for _ in range(self.numProcs):
            self.put(None)

