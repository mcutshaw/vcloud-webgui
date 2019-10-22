import multiprocessing
import configparser
from db import vcloud_db
import datetime

from vcloud import vcloud
import time


class dbManager:
    def __init__(self, config, numProcs=8):

        self.db = vcloud_db(config)
        self.jfac = jobFactory(config, numProcs=numProcs)
        self.config = config

    def _scanForReadyJobs(self):
        readyJobs = self.db.getReadyJobs()
        
        for job in readyJobs:
            tasks = self.db.getTasksByJob(job[0])
            for task in tasks:
                if self.checkTaskReady(task):
                    self.db.updateTaskStatus(task[0],'QUEUED')
                    self.jfac.put(task)
    
    def checkTaskReady(self, taskTup):
        _supportedOps = ['CREATE', 'CHOWN', 'DELETE', 'CAPTURE', 'SNAPSHOT', 'MPOWER', 'UNDO', 'ADDUSER', 'UPDATEDB']
        operation = taskTup[2]
        status = taskTup[5]
        taskDependsID = taskTup[4]
        if status != 'READY':
            return False
        if operation not in _supportedOps:
            return False
        if taskDependsID is not None:
            deptask = self.db.getTaskByID(taskTup[4])
            if deptask[5] != 'COMPLETEDWERROR' or deptask[5] != 'COMPLETED':
                # if deptask[5] == 'FAILED':
                #     self.db.updateTaskStatus(taskTup[1], 'FAILED')
                return False
        return True
            
    def _updateInfoTables(self):
        infoTableKey = 'InfoTables'
        if 'vusers' in self.config[infoTableKey].keys():
            vusersInterval = self.config[infoTableKey]['vusers']
        else:
            vusersInterval = self.config[infoTableKey]['Default']

        if 'vtasks' in self.config[infoTableKey].keys():
            vtasksInterval = self.config[infoTableKey]['vtasks']
        else:
            vtasksInterval = self.config[infoTableKey]['Default']

        if 'vevents' in self.config[infoTableKey].keys():
            veventsInterval = self.config[infoTableKey]['vevents']
        else:
            veventsInterval = self.config[infoTableKey]['Default']

        if 'vtemplates' in self.config[infoTableKey].keys():
            vtemplatesInterval = self.config[infoTableKey]['vtemplates']
        else:
            vtemplatesInterval = self.config[infoTableKey]['Default']

        if 'vapps' in self.config[infoTableKey].keys():
            vappsInterval = self.config[infoTableKey]['vapps']
        else:
            vappsInterval = self.config[infoTableKey]['Default']

    def _updateJobStatuses(self):
        readyJobs = self.db.getReadyJobs()
        for job in readyJobs:
            tasks = self.db.getTasksByJob(job[0])
            statuses = [task[5] for task in tasks]
            if 'RUNNING' in statuses or  'QUEUED' in statuses or 'READY' in statuses:
                continue
            elif 'FAILED' in statuses:
                self.db.updateJobStatus(job[0], 'FAILED')
            elif 'COMPLETEDWERROR' in statuses:
                self.db.updateJobStatus(job[0], 'COMPLETEDWERROR')
            else:
                self.db.updateJobStatus(job[0], 'COMPLETED')

    
    
    def run(self):
        self._scanForReadyJobs()
        self._updateInfoTables()
        self._updateJobStatuses()


class jobFactory:
    def __init__ (self, config, numProcs=8):
        self.config = config
        self.vcloud = vcloud(config)
        self.catalog = self.vcloud.getCatalog(config['Extra']['Catalog'])
        self.vdc = self.vcloud.getVdc(config['Extra']['Vdc'])
        self.org = self.vcloud.getOrg(config['Main']['Org'])
        self.role = self.org.getRole(config['Deploy']['Role'])

        self.numProcs = numProcs
        self.queue = multiprocessing.Queue()
        self.createProcs()

        self.db = vcloud_db(config)

    def createProcs(self):
        for _ in range(self.numProcs):
            p = multiprocessing.Process(target=self._work, args=(config,))
            p.start()

    def _work(self, config):
        db = vcloud_db(config)
        while(True):
            task = self.queue.get()
            if task is None:
                print("Dying gracefully")
                break

            task_op =  task[2]

            db.updateTaskStatus(task[0], "RUNNING")

            if task_op == 'CAPTURE':
                self._tryAndLog(task, self._capture)
            elif task_op == 'UPDATEDB':
                self._tryAndLog(task, self._updateInfoTable)
            else:
                print("Unknown call", task[2])
                print("Discarding...", task[2])
                db.updateTaskStatus(task[0], "FAILED")
                db.updateTaskLog(task[0], "Unknown task type, discarded by main manager")

    def _capture(self, task):
        task_id = task[0]
        args = task[3].split(',')
        vapp_name = args[0]
        template_name = args[1]

        self.db.updateTaskStartedDate(task_id, datetime.datetime.now())

        vapps = self.vcloud.getvApps(vapp_name)

        if vapps != []:
            vapp = vapps[0]
        else:
            self._failTask(task, f"No vapp with name {vapp_name}")
            return None
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
            db.updateTaskLog(task_id, reason)
        self.db.updateTaskCompletedDate(task_id, datetime.datetime.now())
        
    def put(self, item):
        self.queue.put(item)

    def stop(self):
        for _ in range(self.numProcs):
            self.put(None)

config = configparser.ConfigParser()
config.optionxform = str
config.read('vcloud.conf')

# db = vcloud_db(config)
# jobID = db.insertJobs('Test Job', start_date=datetime.datetime.now()+datetime.timedelta(days=2))
# taskID = db.insertTasks(jobID, "CAPTURE", arguments='TESTC,TESTC')
# db.updateTaskStatus(taskID, 'READY')
# db.updateJobStatus(jobID, 'READY')

numProcs = 8

config = configparser.ConfigParser()
config.optionxform = str

config.read('vcloud.conf')
dbM = dbManager(config)
dbM.run()