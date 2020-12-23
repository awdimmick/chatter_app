import sqlite3, abc, time, datetime, random

DB = None


def connect_db(db_path=None):

    if not db_path:
        db_path = 'chatter.db'

    db = sqlite3.connect(db_path, detect_types=sqlite3.PARSE_DECLTYPES)
    db.row_factory = sqlite3.Row

    return db


class InitDB():

    @staticmethod
    def initialise(db_path=None, override_warnings=False):

        db = connect_db(db_path)
        try:

            if not override_warnings:

                conf_number = random.randint(100000,999999)

                print(f"WARNING: Continuing will erase all contents in the database '{db_path}'.\nPlease ensure that you have a recent backup of the database file!")
                if int(input(f"Please enter the number {conf_number} to continue: ")) != conf_number:
                    raise ValueError

            c = db.cursor()

            # REMOVE EXISTING USER TABLE
            c.execute("DROP TABLE IF EXISTS User")

            # CREATE USER TABLE
            sql = '''CREATE TABLE IF NOT EXISTS User (
                            userid INTEGER PRIMARY KEY AUTOINCREMENT,
                            username TEXT UNIQUE NOT NULL,
                            password TEXT NOT NULL,
                            last_login_ts NUMERIC,
                            admin INTEGER DEFAULT 0,
                            active INTEGER DEFAULT 1)
                      '''
            c.execute(sql)

            # Add 'deleteduser' entry to assign messages to when a user is deleted later
            c.execute("INSERT INTO User VALUES (0,'DeletedUser','',0,0,0)")

            # REMOVE EXISTING CHATROOM TABLE
            c.execute("DROP TABLE IF EXISTS Chatroom")

            # CREATE NEW CHATROOM TABLE
            sql = '''CREATE TABLE IF NOT EXISTS Chatroom (
                        chatroomid INTEGER PRIMARY KEY AUTOINCREMENT,
                        name TEXT UNIQUE NOT NULL,
                        description TEXT NOT NULL
                    )
            '''
            c.execute(sql)

            c.execute("DROP TABLE IF EXISTS ChatroomMember")

            sql = '''CREATE TABLE IF NOT EXISTS ChatroomMember(
                        chatroomid INTEGER NOT NULL,
                        userid INTEGER NOT NULL,
                        owner INTEGER DEFAULT 0,
                        PRIMARY KEY (chatroomid, userid),
                        FOREIGN KEY (userid) references User(userid)
                    )'''

            c.execute(sql)

            c.execute("DROP TABLE IF EXISTS Message")

            sql = '''CREATE TABLE IF NOT EXISTS Message (
                    messageid INTEGER PRIMARY KEY AUTOINCREMENT,
                    content TEXT NOT NULL,
                    chatroomid INTEGER NOT NULL,
                    senderid INTEGER NOT NULL,
                    timestamp NUMERIC,
                    FOREIGN KEY (chatroomid) REFERENCES Chatroom(chatroomid),
                    FOREIGN KEY (senderid) REFERENCES User(userid)
            )'''

            c.execute(sql)

            c.execute("DROP TABLE IF EXISTS Attachment")

            sql = '''CREATE TABLE IF NOT EXISTS Attachment (
                    
                        attachmentid INTEGER PRIMARY KEY AUTOINCREMENT,
                        messageid INTEGER NOT NULL,
                        filepath TEXT NOT NULL,
                        FOREIGN KEY (messageid) REFERENCES Message(messageid)
                    
                    )'''

            c.execute(sql)

            db.commit()

            print("SUCCESS: Database initialised with empty tables.")

        except:
            print("Database initialisation aborted.")
            db.rollback()
            raise Exception("ERROR: Incorrect confirmation value entered")


class ChatterDB(abc.ABC):
    # All database objects should have CRUD methods

    # All methods require a connection to the database to be given as these will ultimately be called by different
    # threaded connection from Flask, therefore we cannot pass a single connection that gets reused between instances
    # of each class.

    def __init__(self, id, db):
        pass

    @staticmethod
    def create(self, db):
        # This method is static so that new instances of entities can be created without needing to instantiate
        # an object of that type first.
        pass

    def retrieve(self, userid):
        pass

    def update(self):
        pass

    def delete(self):
        pass


class UserNotFound(Exception):
    pass

class UserPasswordIncorrect(Exception):
    pass

class UserNotAuthorised(Exception):
    pass

class UserDeletionError(Exception):
    pass

class UserCreationError(Exception):
    pass

class User(ChatterDB):

    def __init__(self, id, db:sqlite3.Connection):

        self.__userid = id
        self.db = db

        c = self.db.cursor()

        user_data = c.execute('SELECT username, last_login_ts, admin, active FROM User WHERE userid=?',[self.__userid]).fetchone()

        if user_data:
            self.__username = user_data['username']
            self.__last_login_ts = user_data['last_login_ts']
            self.__admin = True if user_data['admin'] == 1 else False
            self.__active = True if user_data['active'] == 1 else False

        else:
            raise UserNotFound(f"ERROR: No user found with userid {id}.")

    @property
    def admin(self):
        return self.__admin

    @property
    def username(self):
        return self.__username

    @property
    def last_login_ts(self):
        return datetime.datetime.fromtimestamp(self.__last_login_ts)

    def retrieve(self, userid):

        return User(userid, self.db)

    def update(self ):

        """
        This method will update the data in the database for the current User object. ID cannot be modified, username
        can only be updated if not already taken (and is not advised), admin status and password updates are separately
        handled.

        :param db: connection to database
        :return: none
        """

        c = self.db.cursor()

        try:
            c.execute("UPDATE User SET username=?, last_login_ts=? WHERE userid=?", [self.__username, self.__last_login_ts, self.__userid])
            self.db.commit()

        except Exception as e:
            self.db.rollback()
            raise e

    def delete(self, authorised_user):
        """
        Deleting a user requires some checks to be made first, otherwise data integrity issues can arise.
        Specifically, we need to ensure that there will be no orphaned messsages or chatrooms when the user is removed.

        Messages are relatively easy to deal with - we can just assign each senderid to 0, which is a special user
        designated as a 'deleted user' - that way messages can still appear in chatrooms but instead of showing the
        name of the sender, they will just show 'deleted user' as the sender.

        Chatrooms are slightly more complicated. If the user to be deleted is the only owner of a chatroom then that room
        will become partially orphaned as there will be no owner (though existing members will be able to contiue accessing
        it). To fix this, another owner must be assigned to the chatroom before the current user can be deleted.  This
        method will check all chatroom memberships to identify any where the current user is an owner, it will then
        iterate through each of these chatrooms and check to see if there are any other users assigned to those rooms as owners
        other than the present user. If none are found then an exception will be raised on the current user deletion process
        will be aborted.

        Only when no chatrooms are found where the current user is the only owner will deletion take place.

        In addition, user deletion requires that a user object for an admin user is passed as a parameter to authorise
        the deletion. In the web-app context, this will be the active user. If that active user is not an admin then
        the process will fail, raising a UserNotAuthorised exception.

        :param authorised_user: User object for admin user
        :return: none
        """
        c = self.db.cursor()

        try:

            if authorised_user.admin:

                # First check that there are no chatrooms where user is the only owner - fail if so

                chatrooms_owned = c.execute("SELECT chatroomid FROM ChatroomMember WHERE userid=? AND owner=1",[self.__userid]).fetchall()

                for row in chatrooms_owned:

                    chatroom_data = c.execute("SELECT userid FROM ChatroomMember WHERE chatroomid=? and owner=1 and userid != ?", [row['chatroomid'],self.__userid]).fetchone()

                    if not chatroom_data:
                        # There are no other owners for the chatrooms that the present user owns - cannot delete user until other owners are assigned to these chatrooms.
                        raise UserDeletionError(f"Cannot delete user with id {self.__userid} as they are the sole owner of at least one chatroom. Ensure all chatrooms are deleted or assigned to other owner users before deleting this user.")

                # Next, reassign all messages and attachments related to user to special 'Deleted user'
                c.execute("UPDATE Message SET senderid=0 WHERE senderid=?", [self.__userid])

                # Finally, delete use
                c.execute("DELETE FROM User WHERE userid=?", [self.__userid])
                self.db.commit()

            else:
                raise UserNotAuthorised("Only admin users can delete users.")

        except Exception as e:
            self.db.rollback()
            print(e)

    def update_password(self, old_pwd, new_pwd):

        c = self.db.cursor()

        try:

            if len(new_pwd < 8):
                raise UserPasswordIncorrect("Password too short, must be at least 8 characters in length.")

            # Confirm the old_pwd matches the password for this user as stored in the database
            user_data = c.execute("SELECT userid FROM User WHERE userid=? AND password=?",[self.__userid, old_pwd]).fetchone()

            if user_data:
                # old_pwd matches stored password, so update password with new one.
                c.execute("UPDATE User SET password=? WHERE userid=?", [new_pwd, self.__userid])
                self.db.commit()

            else:
                raise UserPasswordIncorrect("ERROR: Old password does not match that stored in database. Cannot update "
                                            "with new password.")

        except Exception as e:
            self.db.rollback()
            print(e)

    def make_admin(self, authorised_user, revoke=False):

        """
        This method will make the current user an admin, but can only be invoked by another user who is already an admin.
        :param authorised_user: User object for an admin user.
        :param db: connection to database
        :param revoke: revokes admin status if set to True (default is False)
        :return: none
        """

        try:

            c = self.db.cursor()

            if authorised_user.admin:

                self.__admin = not revoke
                c.execute("UPDATE User SET admin=? WHERE userid=?", [int(self.__admin), self.__userid])
                self.db.commit()

            else:
                raise UserNotAuthorised("This action can only be performed by an existing administrator.")

        except Exception as e:
            self.db.rollback()
            print(e)

    def update_login_time(self):
        self.__last_login_ts = int(time.time())
        self.update()

    @staticmethod
    def create(username, password, db:sqlite3.Connection):

        try:

            if len(password) < 8:
                raise UserCreationError("Password must be at least 8 characters in length.")

            c = db.cursor()

            c.execute("INSERT INTO User VALUES (NULL, ?, ?, 0, 0, 1)", [username, password])

            userid = c.lastrowid

            db.commit()

            return User(userid, db)

        except sqlite3.IntegrityError as e:
            db.rollback()
            raise UserCreationError(f"User with username '{username}' already exists.")


    @staticmethod
    def login(username, password, db:sqlite3.Connection):
        """
        This static method will attempt to log a user in and, if successful, will return an object representing that user.
        Attempts to login as an inactive user will fail.
        :param username: username of user
        :param password: password of user
        :param db: connection to database
        :return: User object representing logged in user
        """
        try:
            c = db.cursor()
            user_data = c.execute("SELECT userid FROM User where username=? and password=? and active=1",[username, password]).fetchone()
            if user_data:
                u = User(user_data['userid'], db)
                u.update_login_time()
                return u
            else:
                raise UserNotAuthorised(f"Could not login user '{username}'. Check password is correct and that the user is active.")

        except Exception as e:
            raise e

    def __str__(self):
        s = f"<User object: userid: {self.__userid}\tusername: {self.__username}\tlast login: {self.last_login_ts}\tactive? {self.__active}\tadmin? {self.__admin}>"
        return s

if __name__ == "__main__":

    InitDB.initialise()