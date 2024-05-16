import sqlite3
from sqlite3 import Error
import os

def create_connection(db_file):
    """ create a database connection to a SQLite database """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        print(f"Connected to {db_file}, sqlite version: {sqlite3.version}")
        return conn
    except Error as e:
        print(e)
    return conn

def execute_sql(conn, sql):
    """Execute sql
    :param conn: Connection object
    :param sql: a SQL script
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(sql)
    except Error as e:
        print(e)

def add_player(conn, player):
    """
    Create a new player into the players table
    :param conn: Connection object
    :param player: tuple containing player details
    :return: player id
    """
    sql = '''INSERT INTO players(name, position, age)
             VALUES(?,?,?)'''
    cur = conn.cursor()
    cur.execute(sql, player)
    conn.commit()
    return cur.lastrowid

def add_club(conn, club):
    """
    Create a new club into the clubs table
    :param conn: Connection object
    :param club: tuple containing club details
    :return: club id
    """
    sql = '''INSERT INTO clubs(player_id, name, country, city)
             VALUES(?,?,?,?)'''
    cur = conn.cursor()
    cur.execute(sql, club)
    conn.commit()
    return cur.lastrowid

def select_all(conn, table):
    """
    Query all rows in the table
    :param conn: the Connection object
    :return:
    """
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM {table}")
    rows = cur.fetchall()

    for row in rows:
        print(row)

    return rows


def select_where(conn, table, **query):
    """
    Query tasks from table with data from **query dict
    :param conn: the Connection object
    :param table: table name
    :param query: dict of attributes and values
    :return:
    """
    cur = conn.cursor()
    qs = []
    values = ()
    for k, v in query.items():
        qs.append(f"{k}=?")
        values += (v,)
    q = " AND ".join(qs)
    cur.execute(f"SELECT * FROM {table} WHERE {q}", values)
    rows = cur.fetchall()

    for row in rows:
        print(row)

    return rows

def update(conn, table, id, **kwargs):
   """
   update status, begin_date, and end date of a task
   :param conn:
   :param table: table name
   :param id: row id
   :return:
   """
   parameters = [f"{k} = ?" for k in kwargs]
   parameters = ", ".join(parameters)
   values = tuple(v for v in kwargs.values())
   values += (id, )

   sql = f''' UPDATE {table}
             SET {parameters}
             WHERE id = ?'''
   try:
       cur = conn.cursor()
       cur.execute(sql, values)
       conn.commit()
       print("OK")
   except sqlite3.OperationalError as e:
       print(e)


def delete_where(conn, table, **kwargs):
    """
    Delete from table where attributes from
    :param conn:  Connection to the SQLite database
    :param table: table name
    :param kwargs: dict of attributes and values
    :return:
    """
    qs = []
    values = tuple()
    for k, v in kwargs.items():
        qs.append(f"{k}=?")
        values += (v,)
    q = " AND ".join(qs)

    sql = f'DELETE FROM {table} WHERE {q}'
    cur = conn.cursor()
    cur.execute(sql, values)
    conn.commit()
    print("Deleted")

def delete_all(conn, table):
    """
    Delete all rows from table
    :param conn: Connection to the SQLite database
    :param table: table name
    :return:
    """
    sql = f'DELETE FROM {table}'
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()
    print("Deleted")

if __name__ == '__main__':
    db_file = 'database.db'
    database_exists = os.path.exists(db_file)
    
    conn = create_connection(db_file)

    if conn is not None:
        if not database_exists:
            create_football_players = """
            CREATE TABLE IF NOT EXISTS players(
            id integer PRIMARY KEY,
            name text NOT NULL,
            position text,
            age integer NOT NULL
            );
            """
            
            create_players_club_sql = """
            CREATE TABLE IF NOT EXISTS clubs(
            id integer PRIMARY KEY,
            player_id integer NOT NULL,
            name VARCHAR(250),
            country TEXT,
            city TEXT,
            FOREIGN KEY (player_id) REFERENCES players(id)
            );
            """
            
            execute_sql(conn, create_football_players)
            execute_sql(conn, create_players_club_sql)
            
            player = ('Lewandowski', 'Striker', 35)
            player_id = add_player(conn, player)
            
            if player_id:
                club = (player_id, 'FC Barcelona', 'Spain', 'Barcelona')
                club_id = add_club(conn, club)
                print(player_id, club_id)

            player2 = ('Palmer', 'Forward', 22)
            player_id2 = add_player(conn, player2)

            if player_id2:
                club = (player_id2, 'FC Chelsea London', 'England', 'London')
                club_id2 = add_club(conn, club)
                print(player_id2, club_id2)
        else:
            print("Database already exists. Skipping table creation and data insertion.") 
            
        select_all(conn, 'players')
        select_where(conn, 'clubs', name='FC Barcelona')

        update(conn, 'players', 2, position='Winger')

        # delete_where(conn, "players", id=2)
        # delete_all(conn, "clubs")
        
        conn.close()
