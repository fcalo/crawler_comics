from flask import Flask

def create_app():
    app = Flask(__name__)
    
    from application.blueprints.newsletter import newsletter
    app.register_blueprint(newsletter)

    #~ app.config.from_object("settings.Config")
    #~ app.config.from_pyfile(config_filename)

    #~ from yourapplication.model import db
    #~ db.init_app(app)

    
    return app

