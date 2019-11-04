from db import vcloud_db


class User:
    def __init__(self, username, config):
        self.db = vcloud_db(config)
        self.username = username
   
    @property
    def is_active(self):
        return self.db.checkUserActive(self.username)

    @property
    def is_authenticated(self):
        return True

    @property
    def is_anonymous(self):
        return False

    def get_id(self):
        return self.username
    