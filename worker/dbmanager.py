import multiprocessing
import configparser
from db import vcloud_db
import datetime
import time
from jobfactory import jobFactory


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
        tabledict = {}
        if 'vusers' in self.config[infoTableKey].keys():
            tabledict['vusersInterval'] = self.config[infoTableKey]['vusers']
        else:
            tabledict['vusersInterval'] = self.config[infoTableKey]['Default']

        if 'vtasks' in self.config[infoTableKey].keys():
            tabledict['vtasksInterval'] = self.config[infoTableKey]['vtasks']
        else:
            tabledict['vtasksInterval'] = self.config[infoTableKey]['Default']

        if 'vevents' in self.config[infoTableKey].keys():
            tabledict['veventsInterval'] = self.config[infoTableKey]['vevents']
        else:
            tabledict['veventsInterval'] = self.config[infoTableKey]['Default']

        if 'vtemplates' in self.config[infoTableKey].keys():
            tabledict['vtemplatesInterval'] = self.config[infoTableKey]['vtemplates']
        else:
            tabledict['vtemplatesInterval'] = self.config[infoTableKey]['Default']

        if 'vapps' in self.config[infoTableKey].keys():
            tabledict['vappsInterval'] = self.config[infoTableKey]['vapps']
        else:
            tabledict['vappsInterval'] = self.config[infoTableKey]['Default']

        self.db.getLastInfoTableTask('vapps')


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


