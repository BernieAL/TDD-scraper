from flask import Flask
from flask_migrate import Migrate
from config import Config
from models import db, User
from flask_login import LoginManager

def create_app():
    
    app = Flask(__name__)
    app.config.from_object(Config)
    
    # Initialize extensions
    db.init_app(app)
    migrate = Migrate(app, db)
    
    # Initialize Login Manager
    login_manager = LoginManager()
    login_manager.login_view = 'auth.login'
    login_manager.init_app(app)
    
    @login_manager.user_loader
    def load_user(user_id):
        return User.query.get(int(user_id))
    
    # Register blueprints
    from auth import auth_bp
    from search import search_bp
    
    app.register_blueprint(auth_bp)
    app.register_blueprint(search_bp)
    
    return app