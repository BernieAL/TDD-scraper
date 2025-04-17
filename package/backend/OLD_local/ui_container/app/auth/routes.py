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
    print("Registration attempt started")  # Debug

    if form.validate_on_submit():
        print(f"Form validated, email: {form.email.data}")  # Debug
        
        # Check if email exists
        if User.get_by_email(form.email.data):
            print(f"Email {form.email.data} already exists")  # Debug
            flash('Email already registered')
            return redirect(url_for('auth.register'))
        
        try:
            print("Attempting to create new user")  # Debug
            user = User.create_user(
                email=form.email.data,
                password=form.password.data
            )
            print(f"User created successfully with id: {user.id}")  # Debug
            flash('Registration successful')
            return redirect(url_for('auth.login'))
            
        except Exception as e:
            print(f"Registration failed with error: {str(e)}")  # Debug
            flash('Registration failed')
            return redirect(url_for('auth.register'))
    else:
        print("Form validation failed:")  # Debug
        for field, errors in form.errors.items():
            print(f"Field {field} has errors: {errors}")
    
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