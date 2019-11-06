from flask_wtf import FlaskForm
from wtforms import StringField, PasswordField, SubmitField, BooleanField

class LoginForm(FlaskForm):
    username = StringField('Username')
    password = PasswordField('Password')
    submit = SubmitField('Submit')
    remember_me = BooleanField('Remember Me')

class CaptureForm(FlaskForm):
    name = StringField('vApp Name')
    submit = SubmitField('Submit')
    error_override = BooleanField('Error Override')

class UsersForm(FlaskForm):
    name = StringField('Add new user')
    submit = SubmitField('Submit')
