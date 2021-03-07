from sqlite3 import Connection as sqlite3Connection
import time, datetime
from chatter import chatterDB

# Use a test database to avoid altering real data when running tests
TEST_DB_PATH = 'test_db.db'
DB = chatterDB.connect_db(TEST_DB_PATH)


def init_db():

    try:

        chatterDB.InitDB.initialise(TEST_DB_PATH,override_warnings=True)

        init_users()
        init_chatrooms()
        init_chatroom_members()
        init_messages()
        init_attachments()
        DB.commit()

    except Exception as e:
        DB.rollback()
        print(e)


def init_users():

    # REMOVE EXISTING USER TABLE
    c = DB.cursor()

    # INSERT TEST USERS
    sql = '''INSERT INTO User VALUES
                    (NULL,'TestUser1','test1',0,0,1),
                    (NULL,'TestUser2','test2',0,0,1),
                    (NULL,'TestUser3','test3',0,0,1),
                    (NULL,'TestUser4','test4',0,0,1),
                    (NULL,'TestUser5','test5',0,0,1),
                    (NULL,'TestAdmin','testadmin',0,1,1)        
        '''
    c.execute(sql)
    DB.commit()


def init_chatrooms():

    c = DB.cursor()

    # INSERT TEST CHATROOMS
    sql = '''INSERT INTO Chatroom VALUES 
                (NULL,'TestRoom1','A test chatroom'),
                (NULL,'TestRoom2','A test chatroom'),
                (NULL,'TestRoom3','A test chatroom')
    '''

    c.execute(sql)


def init_chatroom_members():

    c = DB.cursor()

    '''
    INSERT CHATROOM MEMBERS as follows:
    
    TestRoom1 (1) has following members:
        TestUser1 (owner), TestUser2, TestUser3
    TestRoom2 (2) has following members:
        TestUser2 (owner), TestUser3, TestUser4
    TestRoom3 (3) has following members:
        TestUser1 (owner), TestUser2(owner), TestUser3, TestUser4, TestUser5
    
    '''
    sql = '''INSERT INTO ChatroomMember VALUES 
            (1, 1, 1),
            (1, 2, 0),
            (1, 3, 0),
            (2, 2, 1),
            (2, 3, 0),
            (2, 4, 0),
            (3, 1, 1),
            (3, 2, 1),
            (3, 3, 0),
            (3, 4, 0),
            (3, 5, 0)
            '''

    c.execute(sql)


def init_messages():

    c = DB.cursor()

    base_time = int(time.time()) - 3600  # Set the message timestamp to be 1 hour ago

    sql =  '''INSERT INTO Message VALUES 
                (NULL, 'This is the first message in TestRoom1, sent by TestUser1. It has two attachments (a picture of Donald Trump and another of Gary Barlow).', 1, 1, ?),
                (NULL, 'This is the second message in TestRoom1, sent by TestUser2.', 1, 2, ?),
                (NULL, 'This is the third message in TestRoom1, sent by TestUser3.', 1, 3, ?),
                (NULL, 'This is the fourth message in TestRoom1, sent by TestUser1.', 1, 1, ?),
                (NULL, 'This is the fifth message in TestRoom1, sent by TestUser3.', 1, 3, ?),
                (NULL, 'This is the sixth message in TestRoom1, sent by TestUser2.', 1, 2, ?)
            '''

    c.execute(sql,[base_time, base_time + 10, base_time + 20, base_time + 30, base_time + 40, base_time + 50])

    sql =  '''INSERT INTO Message VALUES 
                    (NULL, 'This is the first message in TestRoom2, sent by TestUser2.', 2, 2, ?),
                    (NULL, 'This is the second message in TestRoom2, sent by TestUser3.', 2, 3, ?),
                    (NULL, 'This is the third message in TestRoom2, sent by TestUser4.', 2, 4, ?),
                    (NULL, 'This is the fourth message in TestRoom2, sent by TestUser3.', 2, 3, ?),
                    (NULL, 'This is the fifth message in TestRoom2, sent by TestUser4.', 2, 4, ?),
                    (NULL, 'This is the sixth message in TestRoom2, sent by TestUser2. It has an attachment (a picture of Will Smith).', 2, 2, ?)
                '''

    c.execute(sql,[base_time, base_time + 10, base_time + 20, base_time + 30, base_time + 40, base_time + 50])

    sql =  '''INSERT INTO Message VALUES 
                    (NULL, 'This is the first message in TestRoom3, sent by TestUser1.', 3, 1, ?),
                    (NULL, 'This is the second message in TestRoom3, sent by TestUser2. It has an attachment (a picture of Jennifer Anniston).', 3, 2, ?),
                    (NULL, 'This is the third message in TestRoom3, sent by TestUser3. It also has an attachment, this time a picture of Gary Barlow.', 3, 3, ?),
                    (NULL, 'This is the fourth message in TestRoom3, sent by TestUser4.', 3, 4, ?),
                    (NULL, 'This is the fifth message in TestRoom3, sent by TestUser5.', 3, 5, ?),
                    (NULL, 'This is the sixth message in TestRoom3, sent by TestUser4.', 3, 4, ?),
                    (NULL, 'This is the seventh message in TestRoom3, sent by TestUser2.', 3, 2, ?),
                    (NULL, 'This is the eighth message in TestRoom3, sent by TestUser3.', 3, 3, ?),
                    (NULL, 'This is the ninth message in TestRoom3, sent by TestUser1.', 3, 1, ?)
                '''

    c.execute(sql,[base_time, base_time + 10, base_time + 20, base_time + 30, base_time + 40, base_time + 50, base_time + 60, base_time + 70, base_time + 80])


def init_attachments():

    c = DB.cursor()

    sql = '''INSERT INTO Attachment VALUES 
            (NULL, 1, 'donald.png'),
            (NULL, 1, 'gary.png'),
            (NULL, 12, 'will.png'),
            (NULL, 14, 'jen.png'),
            (NULL, 15, 'gary.png')
            
        '''

    c.execute(sql)

def test_messages():

    for x in range(1,22):
        m = chatterDB.Message(x, DB)
        print(m)
        for attachment in m.attachments:
            print(attachment)

    m.delete()
    print(m)

    new_message = chatterDB.Message.create("This is a new message", 1, 1, DB)

    print(new_message)

def test_chatrooms():
    cr = chatterDB.Chatroom(1, DB)
    u = chatterDB.User(1, DB)

    print("Members")
    for member in cr.members:
        print(member)

    print("Owners")
    for owner in cr.owners:
        print(owner)

    print(cr)

    for message in cr.messages:
        print(message)

    pass

if __name__ == "__main__":
    init_db()
    test_messages()
    test_chatrooms()
