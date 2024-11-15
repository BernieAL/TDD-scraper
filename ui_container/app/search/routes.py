# app/search/routes.py
from flask import Blueprint, render_template, redirect, url_for, flash, g
from flask_login import login_required, current_user
from forms import SearchForm
from datetime import datetime
import pika,json

def trigger_scrape(search_id,user_email,brand,category,spec_item):

    """
    triggered when user submits a search
    Search criteria and user email are published to queue
    """
    connection_params = pika.ConnectionParameters(
        host='localhost', 
        port=5672, 
        credentials=pika.PlainCredentials('guest', 'guest')
    )
    connection = pika.BlockingConnection(connection_params)
    channel = connection.channel()
    channel.queue_declare(queue='user_search_requests', durable=True)

    message = {
        'search_id': search_id,
        'user_email': user_email,
        'brand': brand,
        'category': category,
        'spec_item': spec_item
    }

    channel.basic_publish(
        exchange='',
        routing_key='user_search_requests',
        body=json.dumps(message)
    )

    connection.close()



search_bp = Blueprint('search', __name__,template_folder='templates')

@search_bp.route('/')
@login_required
def index():
    return redirect(url_for('search.new_search'))

@search_bp.route('/search', methods=['GET', 'POST'])
@login_required
def new_search():
    form = SearchForm()
    if form.validate_on_submit():
        cur = g.db.cursor()
        try:
            #returns id of newly created search record - s
            # search_id is used in triggering scrape function to associate task with specific search record
            cur.execute("""
                INSERT INTO user_searches 
                (user_id, brand, category, spec_item, timestamp) 
                VALUES (%s, %s, %s, %s, %s)
                RETURNING id 
                """, 
                (current_user.id, 
                 form.brand.data,
                 form.category.data,
                 form.spec_item.data,
                 datetime.utcnow()
                )
            )

            #RETURNING id from query above - returns result set containing specific cols of newly inserted row
            #returns a dictionary-like object (because we're using RealDictCursor) containing the returned columns, and we access the 'id' key to get the value.
            search_id = cur.fetchone()['id']
            g.db.commit()

            #sending form data to be published to queue
            trigger_scrape(
                search_id,
                current_user.email,
                form.brand.data,
                form.category.data,
                form.spec_item.data
            )

            flash('Search request submitted successfully')
            return redirect(url_for('search.history'))
        except Exception as e:
            g.db.rollback()
            flash('Error submitting search request')
        finally:
            cur.close()

    #if form not valid        
    return render_template('search.html', form=form)

@search_bp.route('/history')
@login_required
def history():
    cur = g.db.cursor()
    cur.execute("""
        SELECT id, brand, category, spec_item, timestamp 
        FROM user_searches 
        WHERE user_id = %s 
        ORDER BY timestamp DESC
        """, 
        (current_user.id,)
    )
    searches = cur.fetchall()
    cur.close()
    
    return render_template('history.html', searches=searches)

