from flask import Flask
from flask_login import LoginManager
login_manager = LoginManager()

def create_app():
    app = Flask(__name__):
    app.config.from_object('config.Config')

    #initialize extensions
    db.init_app(app)
    login_manager.init_app(app)

    from .auth.routes import auth_bp
    from .search.routes import search_bp

    app.register_blueprint(auth_bp)
    app.register_blueprint(search_bp)

    return app
