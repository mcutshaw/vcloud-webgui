#!/usr/bin/python3
from datetime import datetime
from configparser import ConfigParser
from flask import Flask, render_template, request, Response, session, redirect, url_for, flash
from functools import wraps
from flask_login import LoginManager, current_user, login_user, logout_user, login_required
from is_safe_url import is_safe_url
from os import urandom

from vcloud import vcloud
from db import vcloud_db
from user import User
from forms import LoginForm, CaptureForm

app = Flask(__name__)      
SECRET_KEY = urandom(32)
app.config['SECRET_KEY'] = SECRET_KEY

config = ConfigParser()
config.read('vcloud.conf')
v = vcloud.vcloud(config)
db = vcloud_db(config)

login_manager = LoginManager()
login_manager.login_view = 'login'
login_manager.init_app(app)

@login_manager.user_loader
def load_user(user_id):
    if db.checkUserExists(user_id):
        return User(user_id, config)
    else:
        return None

def auth(username, password):
    if v.checkAuth(username, password) and db.checkUserActive(username):
        return True
    else:
        return False

@app.route('/login',methods=["POST","GET"])
def login():
    if current_user.is_authenticated:
        return redirect(url_for('capture'))
    form = LoginForm()
    if form.validate_on_submit():
        if auth(form.username.data, form.password.data):
            user = User(form.username.data, config)
        else:
            flash('Invalid username or password')
            return redirect(url_for('login'))
        login_user(user, remember=form.remember_me.data)
        return redirect(url_for('capture'))
    return render_template('login.html', title='Sign In', form=form)

@app.route('/capture',methods=["POST","GET"])
@login_required
def capture():
    form = CaptureForm()
    if form.validate_on_submit():
        error = False
        vapps = v.getvApps(form.name.data)

        if len(vapps) == 0:
            flash('vApp not found')
            return redirect(url_for('capture'))
        else:
            vapp = vapps[0]
    
        if vapp.checkGuestCustomization():
            flash('Guest Customization is enabled, it has a record of breaking vApps')
            error = True

        if vapp.status != 'POWERED_OFF':
            flash('The vApp is not fully powered off ')
            error = True

        if not form.error_override.data and error:
            return redirect(url_for('capture'))
        
        jobID = db.insertJobs(f'{vapp.name} Capture Job')
        taskID = db.insertTasks(jobID, operation="CAPTURE", arguments=f'{vapp.name},{vapp.name}')
        db.updateTaskStatus(taskID, 'READY')
        db.updateJobStatus(jobID, 'READY')

        return render_template('capture.html', title='Capture vApp Templates', submessage='Job Submitted, Monitor progress on normal vCloud interface.', form=form)


    return render_template('capture.html', title='Capture vApp Templates', form=form)


if __name__ == '__main__':
        app.run(host='0.0.0.0', port=5000)
