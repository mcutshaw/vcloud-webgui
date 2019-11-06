#!/usr/bin/python3
import hashlib
import configparser
import pymysql
import datetime

class vcloud_db:

    def __init__(self,config):
        try:
            self.host = config['Database']['Host']
            self.user = config['Database']['User']
            self.database = config['Database']['DB']
            self.password = config['Database']['Password']
            self.port = int(config['Database']['Port'])


        except Exception as e:
            print("Config Error!")
            print(e)
            exit()
        try:    
            self.connect()
        except:
            print("Database Error!")
        self.build()
        

    def build(self):
        tables = self.execute("show tables;")         
        if(('jobs',) not in tables): 
            self.execute('''CREATE TABLE jobs
                            (job_id INT AUTO_INCREMENT,
                            name TEXT,
                            job_depends INT,
                            status ENUM( 'STOPPED', 'COMPLETED', 'COMPLETEDWERROR', 'FAILED', 'READY'),
                            start DATETIME,
                            started_date DATETIME,
                            completed_date DATETIME,
                            cancelable BOOLEAN,
                            CONSTRAINT pk_job_id PRIMARY KEY(job_id),
                            CONSTRAINT fk_job_depends FOREIGN KEY(job_depends) REFERENCES jobs(job_id) ON DELETE CASCADE);''')

        if(('tasks',) not in tables): 
            self.execute('''CREATE TABLE tasks
                            (task_id INT AUTO_INCREMENT,
                            job_id INT,
                            operation ENUM('CREATE', 'CHOWN', 'DELETE', 'CAPTURE', 'SNAPSHOT', 'MPOWER', 'UNDO', 'ADDUSER', 'UPDATEDB') NOT NULL,
                            arguments TEXT NOT NULL,
                            task_depends INT,
                            status ENUM('RUNNING', 'QUEUED','COMPLETED', 'COMPLETEDWERROR', 'FAILED', 'READY'),
                            log TEXT,
                            started_date DATETIME,
                            completed_date DATETIME,
                            CONSTRAINT pk_task_id PRIMARY KEY(task_id),
                            CONSTRAINT fk_job_id FOREIGN KEY(job_id) REFERENCES jobs(job_id) ON DELETE CASCADE,
                            CONSTRAINT fk_task_depends FOREIGN KEY(task_depends) REFERENCES tasks(task_id) ON DELETE CASCADE);''')

        if(('vapps',) not in tables): 
            self.execute('''CREATE TABLE vapps
                            (vapp_id INT AUTO_INCREMENT,
                            name TEXT NOT NULL,
                            status TEXT,
                            owner TEXT,
                            created_date DATETIME,
                            last_opened DATETIME,
                            CONSTRAINT pk_vapp_id PRIMARY KEY(vapp_id));''')

        if(('vtemplates',) not in tables): 
            self.execute('''CREATE TABLE vtemplates
                            (vtemplate_id INT AUTO_INCREMENT,
                            name TEXT NOT NULL,
                            owner TEXT,
                            created_date DATETIME,
                            catalog TEXT NOT NULL,
                            CONSTRAINT pk_vtemplate_id PRIMARY KEY(vtemplate_id));''')

        if(('vevents',) not in tables): 
            self.execute('''CREATE TABLE vevents
                            (vevent_id INT AUTO_INCREMENT,
                            description TEXT NOT NULL,
                            status TEXT,
                            owner TEXT,
                            created_date DATETIME,
                            CONSTRAINT pk_vevent_id PRIMARY KEY(vevent_id));''')

        if(('vtasks',) not in tables): 
            self.execute('''CREATE TABLE vtasks
                            (vtask_id INT AUTO_INCREMENT,
                            description TEXT NOT NULL,
                            status TEXT,
                            owner TEXT,
                            created_date DATETIME,
                            completed_date DATETIME,
                            CONSTRAINT pk_vtask_id PRIMARY KEY(vtask_id));''')

        if(('userlists',) not in tables): 
            self.execute('''CREATE TABLE userlists
                            (userlist_id INT AUTO_INCREMENT,
                            name TEXT NOT NULL,
                            users TEXT,
                            CONSTRAINT pk_userlist_id PRIMARY KEY(userlist_id));''')

        if(('vusers',) not in tables): 
            self.execute('''CREATE TABLE vusers
                            (vuser_id INT AUTO_INCREMENT,
                            username TEXT,
                            email TEXT,
                            CONSTRAINT pk_vuser_id PRIMARY KEY(vuser_id));''')

        if(('users',) not in tables): 
            self.execute('''CREATE TABLE users
                            (user_id INT AUTO_INCREMENT,
                            username TEXT,
                            role TEXT,
                            disabled BOOLEAN,
                            CONSTRAINT pk_user_id PRIMARY KEY(user_id));''')

    
    def close(self):
        self.conn.close()

    def connect(self):
        self.conn = pymysql.connect(host=self.host, user=self.user, password=self.password, db=self.database, port=self.port)
        self.cur = self.conn.cursor()

    def execute(self,command):
        self.connect()
        self.cur.execute(command)
        self.conn.commit()
        text_return = self.cur.fetchall()
        self.close()
        return text_return

    def executevar(self,command,operands):
        self.connect()
        self.cur.execute(command,operands)
        self.conn.commit()
        text_return = self.cur.fetchall()
        self.close()
        return text_return

    ###Jobs
    def insertJobs(self, name, job_depends = None, status='STOPPED', start_date= None, started_date = None, completed_date = None, cancelable = 1):
        if start_date == None:
            start_date = str(datetime.datetime.now())

        self.executevar('INSERT INTO `jobs` (name, job_depends, status, start, started_date, completed_date, cancelable) VALUES(%s,%s,%s,%s,%s,%s,%s)', (name, job_depends, status, start_date, started_date, completed_date, cancelable))
        id = self.cur.lastrowid
        return id
    
    def getJobByName(self, name):
        jobs = self.execute(f'SELECT * FROM `jobs` WHERE name={name}')
        return jobs[0]

    def getAllJobs(self):
        jobs = self.execute('SELECT * FROM `jobs`')
        return [job for job in jobs]
    
    def getReadyJobs(self):
        jobs = self.execute('SELECT * FROM jobs WHERE status=\'READY\' AND (start<=NOW() or start=NULL)')
        return [job for job in jobs]

    def updateJobStatus(self, job_id, status):
        self.execute(f'UPDATE `jobs` SET status=\'{status}\' WHERE job_id={job_id}')

    def updateJobStartedDate(self, job_id, started_date):
        self.execute(f'UPDATE `jobs` SET started_date=\'{started_date}\' WHERE job_id={job_id}')

    def updateJobCompletedDate(self, job_id, completed_date):
        self.execute(f'UPDATE `jobs` SET completed_date=\'{completed_date}\' WHERE job_id={job_id}')

    ###Tasks
    def insertTasks(self, job_id, operation, arguments=None, task_depends=None, status='READY', log=None, started_date=None, completed_date=None):
        self.executevar('INSERT INTO `tasks` (job_id, operation, arguments, task_depends, status, log, started_date, completed_date) VALUES(%s,%s,%s,%s,%s,%s,%s,%s)', (job_id, operation, arguments, task_depends, status, log, started_date, completed_date))
        id = self.cur.lastrowid
        return id
    
    def getTasksByJob(self, job_id):
        tasks = self.execute(f'SELECT * FROM `tasks` WHERE job_id={job_id}')
        return [task for task in tasks]

    def getTaskByID(self, task_id):
        tasks = self.execute(f'SELECT * FROM `tasks` WHERE task_id={task_id}')
        return tasks[0]
    
    def getLastInfoTableTask(self, table):
        tasks = self.execute(f'SELECT * FROM `tasks` WHERE operation=\'UPDATEDB\' AND arguments=\'{table}\' ORDER BY completed_date DESC LIMIT 1')
        if tasks == ():
            return None
        print(tasks)
        return tasks[0]
    
    def updateTaskStatus(self, task_id, status):
        self.execute(f'UPDATE `tasks` SET status=\'{status}\' WHERE task_id={task_id}')

    def updateTaskStartedDate(self, task_id, started_date):
        self.execute(f'UPDATE `tasks` SET started_date=\'{started_date}\' WHERE task_id={task_id}')

    def updateTaskCompletedDate(self, task_id, completed_date):
        self.execute(f'UPDATE `tasks` SET completed_date=\'{completed_date}\' WHERE task_id={task_id}')

    def updateTaskLog(self, task_id, log):
        query = f'UPDATE `tasks` SET log=\'{log}\' WHERE task_id={task_id}'
        print(query)
        self.execute(query)

    ###Users
    def insertUser(self, username, role='admin', disabled=False):
        self.executevar('INSERT INTO `users` (username, role, disabled) VALUES(%s,%s,%s)', (username, role, disabled))
        id = self.cur.lastrowid
        return id
    
    def getUserByName(self, username):
        users = self.execute(f'SELECT * FROM `users` WHERE username=\'{username}\'')
        return [user for user in users]
    
    def getUsers(self):
        users = self.execute(f'SELECT * FROM `users`')
        return [user for user in users]

    def checkUserActive(self, username):
        users = self.execute(f'SELECT * FROM `users` WHERE username=\'{username}\' and disabled=false')
        if len(users) > 0:
            return True
        else:
            return False


    def checkUserExists(self, username):
        users = self.execute(f'SELECT * FROM `users` WHERE username=\'{username}\'')
        if len(users) > 0:
            return True
        else:
            return False

    def deleteUserByName(self, username):
        self.execute(f'DELETE FROM `users` WHERE username=\'{username}\'')
        return None