# app/__init__.py
from flask import Flask, g  # Added g import
from flask_login import LoginManager
import psycopg2
from psycopg2.extras import RealDictCursor
from config.config import Config
from app.models import User  # Add this import

login_manager = LoginManager()

def get_db_connection():
    """Create a database connection"""
    # return psycopg2.connect(
    #     host=Config.DB_HOST,
    #     port=Config.DB_PORT,
    #     database=Config.DB_NAME,
    #     user=Config.DB_USER,
    #     password=Config.DB_PASSWORD,
    #     cursor_factory=RealDictCursor  # Returns results as dictionaries
    # )
    return psycopg2.connect(
        Config.DB_URI,
        cursor_factory=RealDictCursor
    )
def create_app():
    app = Flask(__name__)

    app.config.from_object(Config)

    # Initialize login manager
    login_manager.init_app(app)
    login_manager.login_view = 'auth.login'

    # Add the user loader
    @login_manager.user_loader
    def load_user(user_id):
        return User.get_by_id(int(user_id))

    # Add database connection to app context
    @app.before_request
    def before_request():
        if not hasattr(g, 'db'):
            g.db = get_db_connection()
    
    @app.teardown_appcontext
    def teardown_db(exception):
        db = g.pop('db', None)
        if db is not None:
            db.close()

    # Register blueprints
    from app.auth.routes import auth_bp     # Updated import path
    from app.search.routes import search_bp  # Updated import path
    
    app.register_blueprint(auth_bp,url_prefix='/auth')
    app.register_blueprint(search_bp,url_prefix='/search')

    
    
    return app

