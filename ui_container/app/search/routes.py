# search.py
from flask import Blueprint, render_template, redirect, url_for, flash
from flask_login import login_required, current_user
from app.models import db, Search
from forms import SearchForm

search_bp = Blueprint('search', __name__)

@search_bp.route('/')
@login_required
def index():
    return redirect(url_for('search.new_search'))

@search_bp.route('/search', methods=['GET', 'POST'])
@login_required
def new_search():
    form = SearchForm()
    if form.validate_on_submit():
        search = Search(
            user_id=current_user.id,
            brand=form.brand.data,
            category=form.category.data,
            spec=form.spec.data
        )
        db.session.add(search)
        db.session.commit()
        flash('Search request submitted successfully')
        return redirect(url_for('search.history'))
    return render_template('search/search.html', form=form)

@search_bp.route('/history')
@login_required
def history():
    searches = Search.query.filter_by(user_id=current_user.id)\
        .order_by(Search.timestamp.desc()).all()
    return render_template('search/history.html', searches=searches)