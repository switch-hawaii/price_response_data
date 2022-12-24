from flask import Flask, g, url_for, \
     render_template, request, Response
import psycopg2
import csv, itertools

app = Flask(__name__)

# open the database and a cursor
def open_db():
    g.db = psycopg2.connect()
    g.cur = g.db.cursor()
    
# close the database and cursor which were opened earlier
def close_db():
    db = getattr(g, 'db', None)
    if db is not None:
        db.close()
    cur = getattr(g, 'cur', None)
    if cur is not None:
        cur.close()

@app.route('/sites')
@app.route('/wind')
def wind():
    open_db()
    g.cur.execute('select * from turbine order by turbine_id asc')
    rows = g.cur.fetchall()
    cols = [desc[0] for desc in g.cur.description]
    close_db()
    return render_template('wind.html', rows=rows, cols=cols)

@app.route('/wind_data', methods=['POST'])
def wind_data():
    print request.form
    req = request.form['get_data']
    open_db()
    print "starting query"
    if req == "all_sites":
        g.cur.execute('select * from turbine order by turbine_id asc')
    elif req == "all_sites_all_hours":
        g.cur.execute('select turbine_id, timestamp, windpower from turbine_hourly order by turbine_id, timestamp asc')
    else:
        turbine_id = int(req)
        g.cur.execute('select * from turbine_hourly where turbine_id = %s order by timestamp asc', [turbine_id])
    print "finished query"

    # make a single iterator with a header row and all the data
    rows = itertools.chain(
            [tuple(desc[0] for desc in g.cur.description)],
            g.cur
        )

    # Create a response using an iterator that converts the rows into csv.
    # Using an iterator allows flask to stream the data, rather than manipulating a giant table.
    # (The downside is, the user won't know how large the file will become until it finishes.)
    # Include headers to force the result to be downloaded instead of displayed.
    return Response(
        csv_iterator(rows),
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment;filename={name}.csv".format(name=req)}
    )

def csv_iterator(data):
    # convert iterable data to csv (in a buffer) and then yield the result
    buf = FIFOBuffer()
    writer = csv.writer(buf)
    for row in data:
        writer.writerow(row)
        yield buf.read()
    
    # this is a strange place for it, but it seems to be the best opportunity to close the database/cursor
    # also note: this has to be done inside an application context for some reason
    # (see https://github.com/mattupstate/flask-mail/issues/63)
    # note: another option might be to create a custom Response class, which
    # does the row -> csv conversion and also calls close_db() from its destructor
    with app.app_context():
        close_db()

class FIFOBuffer(object):
    def __init__(self):
        self.buf = ""
    def write(self, text):
        self.buf = self.buf + text
    def read(self):
        text = self.buf
        self.buf = ""
        return text


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=False)
