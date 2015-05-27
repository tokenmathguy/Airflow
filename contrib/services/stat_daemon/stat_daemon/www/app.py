import argparse
import datetime
import dateutil.parser as dparser
import json
import logging
import re
import urllib

import numpy as np
import pandas as pd

from airflow.settings import Session
from airflow.hooks import MySqlHook, SqliteHook
from flask import (Flask, flash, redirect, render_template,
                   request, url_for)
from flask.ext.admin import Admin, BaseView, expose, AdminIndexView
from flask.ext.admin.contrib.sqla import ModelView
from werkzeug.contrib.cache import SimpleCache
from chart import highchart_timeseries

import stat_daemon.tsa

cache = SimpleCache(default_timeout=1)


def cache_key(request):
    return request.base_url + str(request.args)


def _get_sql_hook(sql_conn_id):
    """
    Local helper function to get a SQL hook
    """
    if 'sqlite' in sql_conn_id:
        return SqliteHook(sql_conn_id)
    else:
        return MySqlHook(sql_conn_id)


class TimeSeries(BaseView):

    def __init__(self, name='Time Series', category='Metadata',
                 table="metadata", sql_conn_id="stat_daemon"):
        """
        """
        super(TimeSeries, self).__init__(name=name, category=category)
        self.paths = []
        self.table_name = table
        self.sql_conn_id = sql_conn_id

    def get_datetime(self, text):
        """
        """
        default = datetime.datetime.utcnow() + datetime.timedelta(days=1)
        default.replace(hour=0, minute=0, second=0, microsecond=0)
        try:
            ds = dparser.parse(text, fuzzy=True, default=default)
            if len([m.start() for m in re.finditer(':', text)]) < 2:
                return ds.replace(hour=0, minute=0, second=0, microsecond=0)
            else:
                return ds
        except:
            return default

    def parse_float(self, str_val):
        """
        Attempt to parse a string as float
        """
        try:
            return float(str_val)
        except:
            return None

    def parse_source(self, request):
        """
        """
        source = request.args.get('source', None)
        resp = None
        if source:
            source = source.replace(' ', '')
        else:
            flash("Error, source undefined; retrying Step 1.")
            resp = redirect('/admin/timeseries/wizard/step1')
        return source, resp

    def parse_stat(self, request):
        """
        """
        stat = request.args.get('stat', None)
        resp = None
        if not stat:
            flash("Error, stat undefined; retrying Step 2.")
            resp = redirect('/admin/timeseries/wizard/step2')
        return stat, resp

    @expose('/')
    def index(self):
        return redirect('/admin/timeseries/wizard')

    @expose('/wizard')
    def wizard(self):
        return redirect('/admin/timeseries/wizard/step1')

    @expose('/wizard/step1')
    def wizard_step1(self):
        if cache.get(cache_key(request)):
            return cache.get(cache_key(request))
        else:
            # hard code these for now...
            data = ['test:stat_daemon', 'core_data.fct_bookings']
            rendered_template = self.render('wizard.html',
                                            data=data, step=1)
            cache.set(cache_key(request), rendered_template)
            return rendered_template

    @expose('/wizard/step2')
    def wizard_step2(self):
        if cache.get(cache_key(request)):
            return cache.get(cache_key(request))
        else:
            source, resp = self.parse_source(request)
            if not source:
                return resp
            table = self.table_name
            db = _get_sql_hook(self.sql_conn_id)
            sql = """\
            SELECT
                DISTINCT(stat)
            FROM
                v_{table}
            WHERE
                path LIKE '%{source}%'
            LIMIT
                100
            ;
            """.format(**locals())
            logging.info(sql)
            rows = cache.get(sql)
            if not rows:
                rows = db.get_records(sql)
                cache.set(sql, rows)
            if not rows:
                flash("Error, source returned no records; retrying Step 1.")
                return redirect('/admin/timeseries/wizard/step1')
            data = [str(row[0]) for row in rows]
            rendered_template = self.render('wizard.html',
                                            data=data, step=2)
            cache.set(cache_key(request), rendered_template)
            return rendered_template


    @expose('/plot')
    def plot(self):
        if cache.get(cache_key(request)):
            return cache.get(cache_key(request))
        else:
            ts_start = request.args.get('start_date', '2008-05-28')
            start = self.get_datetime(ts_start)
            ts_end = request.args.get('end_date')
            end = self.get_datetime(ts_end)
            yoy = request.args.get('yoy')
            wow = request.args.get('wow')
            max_tol = self.parse_float(request.args.get('max_tol'))              
            min_tol = self.parse_float(request.args.get('min_tol'))
            source, resp = self.parse_source(request)
            if not source:
                return resp
            source = '%{}%'.format(source)
            stat, resp = self.parse_stat(request)
            if not source:
                return resp
            stat = '%{}%'.format(stat)
            table = self.table_name
            db = _get_sql_hook(self.sql_conn_id)
            sql = """\
            SELECT
                path
                , stat
                , val
                , ts
            FROM
                v_{table}
            WHERE
                (path LIKE '{source}')
                AND (stat LIKE '{stat}')
            LIMIT
                10000
            ;
            """.format(**locals())
            logging.info(sql)
            rows = cache.get(sql)
            if not rows:
                rows = db.get_records(sql)
                cache.set(sql, rows)
            if not rows:
                flash(("Error, stat='{}' returned no records; "
                       " retrying Step 2.").format(str(stat)))
                return redirect(("/admin/timeseries/wizard/step2?source=" +
                                 urllib.quote(source, safe='')))
            data = {}
            for path, stat, val, ts in rows:
                time = self.get_datetime(path)
                if time >= start and time <= end:
                    if not stat in data:
                        data[stat] = pd.Series()
                    data[stat][time] = val
            df = pd.DataFrame(data)
            detrend = request.args.get('detrend', False)
            if detrend:
                df = stat_daemon.tsa.detrend(df)
            if not max_tol:
                if 'Residuals' in df.columns:
                    max_tol = df['Residuals'].max()
                else:
                    max_tol = df[df.columns[0]].max()
            if not min_tol:
                if 'Residuals' in df.columns:
                    min_tol = df['Residuals'].min()
                else:
                    min_tol = df[df.columns[0]].min()
            df['max_tol'] = max_tol
            df['min_tol'] = min_tol
            min_min_tol = 2.0*min_tol*(1 if min_tol < 0 else -1)
            min_min_tol = self.parse_float("{0:.2f}".format(min_min_tol))
            max_max_tol = 2.0*max_tol*(1 if max_tol > 0 else -1)
            max_max_tol = self.parse_float("{0:.2f}".format(max_max_tol))
            if not min_min_tol:
                min_min_tol = -1
            if not max_max_tol:
                max_max_tol = 1
            steps = self.parse_float(request.args.get('steps', 100))
            chart = highchart_timeseries(df)
            rendered_template = self.render('time_series.html', 
                chart=chart, max_tol=max_tol, min_tol=min_tol,
                min_min_tol=min_min_tol, max_max_tol=max_max_tol, steps=steps)
            cache.set(cache_key(request), rendered_template)
            return rendered_template


class TagsView(BaseView):

    def __init__(self, name='Tags', category='Metadata',
                 table="metadata", sql_conn_id="stat_daemon"):
        """
        """
        super(TagsView, self).__init__(name=name, category=category)
        self.paths = []
        self.table_name = table
        self.sql_conn_id = sql_conn_id

    @expose('/')
    def index(self):
        pass

# class TagsView(ModelView):
# class AlertsView(ModelView):
# class CaptainHindsightView


class StatDaemon(object):

    def __init__(self, table="metadata", sql_conn_id="stat_daemon"):
        """
        """
        self.table_name = table
        self.sql_conn_id = sql_conn_id

    def get_views(self):
        """
        Generates views that can be incorporated into a Flask app
        """
        views = []
        views.append(TimeSeries(table=self.table_name,
                                sql_conn_id=self.sql_conn_id))
        views.append(TagsView(table=self.table_name,
                              sql_conn_id=self.sql_conn_id))
        return views

    def get_app(self):
        app = Flask(__name__)
        app.secret_key = 'add a real one later'

        @app.route('/')
        def index():
            return redirect(url_for('admin.index'))

        class HomeView(AdminIndexView):

            @expose("/")
            def index(self):
                return redirect('/admin/timeseries/wizard')

        admin = Admin(
            app,
            name="StatDaemon",
            index_view=HomeView(),
            template_mode='bootstrap3')
        admin._menu = []

        for view in self.get_views():
            admin.add_view(view)
        return app

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='StatDaemon Web UI')
    parser.add_argument("--sql_conn_id",
                        default='stat_daemon',
                        help="SQL connection id")
    parser.add_argument("--dest",
                        default='metadata',
                        help="Base name of the stats table")
    args = parser.parse_args()
    app = StatDaemon(table=args.dest, sql_conn_id=args.sql_conn_id).get_app()
    logging.getLogger().setLevel(logging.INFO)
    app.run(debug=True, host='0.0.0.0', port=6969)
