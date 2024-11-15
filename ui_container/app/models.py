from flask_login import UserMixin
from werkzeug.security import generate_password_hash, check_password_hash
from flask import g

class User(UserMixin):
    def __init__(self, id, email, password_hash):  # Changed username to email
        self.id = id
        self.email = email  # Changed from username
        self.password_hash = password_hash

    @staticmethod
    def get_by_id(user_id):
        cur = g.db.cursor()
        cur.execute("SELECT * FROM users WHERE id = %s", (user_id,))
        user_data = cur.fetchone()
        cur.close()

        if user_data:
            return User(
                id=user_data['id'],
                email=user_data['email'],  # Changed from username
                password_hash=user_data['password_hash']
            )
        return None

    @staticmethod
    def get_by_email(email):  # Changed from get_by_username
        cur = g.db.cursor()
        cur.execute("SELECT * FROM users WHERE email = %s", (email,))
        user_data = cur.fetchone()
        cur.close()

        if user_data:
            return User(
                id=user_data['id'],
                email=user_data['email'],  # Changed from username
                password_hash=user_data['password_hash']
            )
        return None
    def __init__(self, id, email, password_hash):
        self.id = id
        self.email = email
        self.password_hash = password_hash

    @staticmethod
    def get_by_id(user_id):
        cur = g.db.cursor()
        cur.execute("SELECT * FROM users WHERE id = %s", (user_id,))  # Fixed: Added comma to make it a tuple
        user_data = cur.fetchone()
        cur.close()

        if user_data:
            return User(
                id=user_data['id'],
                email=user_data['email'],
                password_hash=user_data['password_hash']
            )
        return None

   
    def set_password(self, password):
        self.password_hash = generate_password_hash(password)

    def check_password(self, password):
        return check_password_hash(self.password_hash, password)

@staticmethod
def create_user(email, password):  # Changed from username
    password_hash = generate_password_hash(password)
    
    cur = g.db.cursor()
    try:
        cur.execute(
            "INSERT INTO users (email, password_hash) VALUES (%s, %s) RETURNING id",
            (email, password_hash)
        )
        user_id = cur.fetchone()['id']
        g.db.commit()
        cur.close()
        
        return User(
            id=user_id,
            email=email,  # Changed from username
            password_hash=password_hash
        )
    except Exception as e:
        g.db.rollback()
        cur.close()
        raise e