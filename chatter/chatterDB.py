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
    def create(db):
        # This method is static so that new instances of entities can be created without needing to instantiate
        # an object of that type first.
        pass

    def retrieve(self):
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

    @staticmethod
    def retrieve(userid, db):

        return User(userid, db)

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

class ChatroomNotFound(Exception):
    pass

class Chatroom(ChatterDB):

    # TODO: Add new member, new owner, new message, demote owner, remove member, delete chatroom, create chatroom

    def __init__(self, chatroomid, db:sqlite3.Connection):

        self.__chatroomid = chatroomid
        self.__owners = []
        self.__members = []
        self.__messages = []

        self.db = db

        c = db.cursor()

        chatroom_data = c.execute("SELECT name, description FROM Chatroom WHERE chatroomid=?", [self.__chatroomid]).fetchone()

        if chatroom_data:
            self.__name = chatroom_data['name']
            self.__description = chatroom_data['description']
            self.__update_owners()
            self.__update_members()

        else:
            raise ChatroomNotFound(f"No chatroom found with chatroomid {self.__chatroomid}")

    def __update_members(self):

        c = self.db.cursor()
        members_data = c.execute("SELECT userid FROM ChatroomMember WHERE chatroomid=? AND owner=0", [self.__chatroomid]).fetchall()

        for row in members_data:
            self.__members.append(User(row['userid'], self.db))

    def __update_owners(self):

        c = self.db.cursor()
        owners_data = c.execute("SELECT userid FROM ChatroomMember WHERE chatroomid=? AND owner=1", [self.__chatroomid]).fetchall()

        for row in owners_data:
            self.__owners.append(User(row['userid'], self.db))

    @property
    def name(self):
        return self.__name

    @property
    def description(self):
        return self.__description

    @property
    def messages(self):
        return Message.get_messages_for_chatroom(self.__chatroomid, self.db)

    @property
    def owners(self):
        return self.__owners

    @property
    def members(self):
        return self.__members

    def is_owner(self, user:User):

        for o in self.__owners:
            if o.username == user.username:
                return True

        return False

    def get_messages_since(self, messageid):
        return Message.get_messages_for_chatroom(self.__chatroomid, self.db, messageid)

    def __str__(self):
        return f"<Chatroom object: Chatroomid: {self.__chatroomid}\tOwners: {self.owners}\tNumber of members: {len(self.members)}>"


class MessageNotFound(Exception):
    pass

class InvalidMessageData(Exception):
    pass

class Message(ChatterDB):

    def __init__(self, messageid, db:sqlite3.Connection):

        self.__messageid = messageid
        self.db = db
        self.__attachments = None

        c = db.cursor()

        message_data = c.execute("SELECT content, chatroomid, senderid, timestamp FROM Message WHERE messageid=?", [self.__messageid]).fetchone()

        if message_data:

           self.__content = message_data['content']
           self.__chatroomid = message_data['chatroomid']
           self.__senderid = message_data['senderid']
           self.__timestamp = message_data['timestamp']
           self.__attachments = Attachment.get_attachments_for_message(self, db)

        else:

            raise MessageNotFound(f"No message found with ID {self.__messageid}")

    @property
    def timestamp(self):
        return datetime.datetime.fromtimestamp(self.__timestamp)

    @property
    def sender(self):
        return User(self.__senderid, self.db)

    @property
    def senderid(self):
        return self.__senderid

    @property
    def content(self):
        return self.__content

    @property
    def messageid(self):
        return self.__messageid

    @property
    def chatroomid(self):
        return self.__chatroomid

    @property
    def chatroom(self):
        #TODO: Implement Chatroom object to return
        return None

    @property
    def attachments(self):
        return self.__attachments

    def delete(self):

        c = self.db.cursor()

        try:
            # Remote all attachments related to message first
            for attachment in self.attachments:
                attachment.delete()

            # Now delete message
            c.execute("DELETE FROM Message WHERE messageid=?", [self.__messageid])
            self.db.commit()


        except Exception as e:
            self.db.rollback()
            raise e


    @staticmethod
    def retrieve(id, db:sqlite3.Connection):
        return Message(id, db)

    @staticmethod
    def create(content, chatroomid, senderid, db:sqlite3.Connection):

        c = db.cursor()

        try:

            #TODO: Add validation of content, chatroomid and senderid

            if len(content) == 0:
                raise InvalidMessageData("Message contains no content")

            c.execute("INSERT INTO Message VALUES (NULL,?,?,?,?)",[content, chatroomid, senderid, int(time.time())])

            messageid = c.lastrowid

            db.commit()

            return Message(messageid, db)

        except Exception as e:
            db.rollback()
            raise e

    def add_attachment(self, filepath):

        try:
            Attachment.create(self.messageid, filepath, self.db)
            self.__update_attachments()

        except Exception as e:

            self.db.rollback()
            raise e

    def __update_attachments(self):
        self.__attachments = Attachment.get_attachments_for_message(self, self.db)

    def __str__(self):
        return f"<Message object: Messageid: {self.messageid}\tFrom: {self.sender.username}\tContent: {self.content}\tParent Chatroom ID: {self.chatroomid}\tTimestamp: {self.timestamp}\tNumber of attachments: {len(self.attachments)}>"

    @staticmethod
    def get_messages_for_chatroom(chatroomid, db:sqlite3.Connection, since_message_id=0):

        c = db.cursor()

        messages = []

        messages_data = c.execute("SELECT messageid FROM Message WHERE chatroomid=? AND messageid > ?", [chatroomid, since_message_id]).fetchall()

        for row in messages_data:
            messages.append(Message(row['messageid'], db))

        return messages

class AttachmentNotFound(Exception):
    pass

class Attachment(ChatterDB):

    def __init__(self, attachmentid, db:sqlite3.Connection):

        self.__attachmentid = attachmentid
        self.db = db

        c = self.db.cursor()

        attachment_data = c.execute("SELECT messageid, filepath FROM Attachment WHERE attachmentid=?", [self.__attachmentid]).fetchone()

        if attachment_data:
            self.__messageid = attachment_data['messageid']
            self.__filepath = attachment_data['filepath']

        else:
            raise AttachmentNotFound(f"No Attachment found with ID {self.__attachmentid}")

    @property
    def attachmentid(self):
        return self.__attachmentid

    @property
    def parent_message(self):
        return Message(self.__messageid, self.db)

    @property
    def messageid(self):
        return self.__messageid

    @property
    def filepath(self):
        return self.__filepath

    def delete(self):
        c = self.db.cursor()

        try:
            c.execute("DELETE FROM Attachment WHERE attachmentid=?", [self.__attachmentid])
            self.db.commit()

        except Exception as e:
            self.db.rollback()
            raise e

    @staticmethod
    def create(messageid, filepath, db:sqlite3.Connection):
        c = db.cursor()
        try:
            c.execute("INSERT INTO Attachment VALUES (NULL, ?, ?)", [messageid, filepath])
            attachment_id = c.lastrowid
            db.commit()
            return Attachment(attachment_id, db)

        except Exception as e:
            db.rollback()
            raise e

    @staticmethod
    def retrieve(attachmentid, db):
        return Attachment(attachmentid, db)

    @staticmethod
    def get_attachments_for_message(message:Message, db:sqlite3.Connection):

        attachments = []

        c = db.cursor()

        attachments_data = c.execute("SELECT attachmentid FROM Attachment WHERE messageid=?", [message.messageid]).fetchall()

        for row in attachments_data:
            attachments.append(Attachment(row['attachmentid'], db))

        return attachments

    def __str__(self):

        return f"<Attachment object: Attachmentid: {self.__attachmentid}\tFilepath: {self.filepath}\tBelongs to messageid {self.messageid}>"

if __name__ == "__main__":

    InitDB.initialise()