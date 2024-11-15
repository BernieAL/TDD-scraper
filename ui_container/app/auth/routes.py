# app/auth/routes.py
from flask import Blueprint, render_template, redirect, url_for, flash, request
from flask_login import login_user, logout_user, login_required
from app.models import User
from flask import g
from forms import LoginForm, RegistrationForm
from datetime import datetime

auth_bp = Blueprint('auth', __name__,
                   template_folder='templates')

@auth_bp.route('/register', methods=['GET', 'POST'])
def register():
    form = RegistrationForm()

    if form.validate_on_submit():
        # Check if email exists
        cur = g.db.cursor()
        cur.execute("SELECT * FROM users WHERE email = %s", (form.email.data,))
        existing_user = cur.fetchone()
        cur.close()

        if existing_user:
            flash('Email already registered')
            return redirect(url_for('auth.register'))
        
        try:
            # Create new user
            cur = g.db.cursor()
            cur.execute("""
                INSERT INTO users (email, password_hash, created_at) 
                VALUES (%s, %s, %s) 
                RETURNING id
                """, 
                (form.email.data, 
                 User.generate_password_hash(form.password.data),
                 datetime.utcnow()
                )
            )
            user_id = cur.fetchone()['id']
            g.db.commit()
            cur.close()
            
            flash('Registration successful')
            return redirect(url_for('auth.login'))
            
        except Exception as e:
            g.db.rollback()
            flash('Registration failed')
            return redirect(url_for('auth.register'))
    
    return render_template('register.html', form=form)

@auth_bp.route('/login', methods=['GET', 'POST'])
def login():
    form = LoginForm()

    if form.validate_on_submit():
        cur = g.db.cursor()
        print(f"Attempting login with email: {form.email.data}")  # Debug
        cur.execute("SELECT * FROM users WHERE email = %s", (form.email.data,))
        user_data = cur.fetchone()
        cur.close()

        if user_data:
            print(f"Found user: {user_data}")  # Debug
            user = User(
                id=user_data['id'],
                email=user_data['email'],
                password_hash=user_data['password_hash']
            )
            
            print(f"Checking password match")  # Debug
            if user.check_password(form.password.data):
                print("Password matched")  # Debug
                login_user(user)
                
                cur = g.db.cursor()
                cur.execute("""
                    UPDATE users 
                    SET last_login = %s 
                    WHERE id = %s
                """, (datetime.utcnow(), user.id))
                g.db.commit()
                cur.close()

                next_page = request.args.get('next')
                return redirect(next_page or url_for('search.index'))
        
        flash('Invalid email or password')
    return render_template('login.html', form=form)
@auth_bp.route('/logout')
@login_required
def logout():
    logout_user()
    return redirect(url_for('auth.login'))