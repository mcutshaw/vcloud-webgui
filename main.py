import configparser
from vcloud import vcloud
from dbmanager import dbManager
import datetime
from db import vcloud_db

config = configparser.ConfigParser()
config.optionxform = str
config.read('vcloud.conf')

db = vcloud_db(config)
# jobID = db.insertJobs('Test Job', start_date=datetime.datetime.now())
# taskID = db.insertTasks(jobID, operation="UPDATEDB", arguments='vapps')
# db.updateTaskStatus(taskID, 'READY')
# db.updateJobStatus(jobID, 'READY')

numProcs = 8

config = configparser.ConfigParser()
config.optionxform = str

config.read('vcloud.conf')
dbM = dbManager(config)
dbM.run()