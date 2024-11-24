import psycopg2
from colorama import Fore as f
from seed import generate_status, generate_tasks, generate_users
from tasks import TASKS
from basic_configs import configs, base_configs


def check_connection(conf):
    """Допоміжна функція, яка використовується у функції connect_db, для підключення до Postgresql"""
    try:
        with psycopg2.connect(**conf) as conn:
            return conn
    except (psycopg2.DatabaseError, Exception) as error:
        return None


def connect_db(base_conf, conf, db_name = 'best_db'):
    """Основна функція для підключення до Postgresql та створення нової БД"""
    db_exists = check_connection(conf)
    if not db_exists:
        try:
            conn = psycopg2.connect(**base_conf)
            conn.autocommit = True
            cur = conn.cursor()
            cur.execute(f"""CREATE DATABASE {db_name};""")
            conn.close()

            new_conn = psycopg2.connect(**conf)
            return new_conn
        
        except (psycopg2.DatabaseError, Exception) as error:
            return error
    else:
        return db_exists


def cur_execmany(con, query, tablename):
    """Функція для виконання курсора для багатьох записів"""
    cur = con.cursor()
    try:
        cur.executemany(query, tablename)
        con.commit()
    except Exception as e:
        print("Couldn't insert the data")
    else:
        print("Data added into table")
    finally:
        cur.close()


def cur_exec(con, query: str, *args):
    """Функція для виконання курсора для одного запису та команд типу Update, Delete"""
    if not args:
        if query.startswith("SELECT"):
            cur = con.cursor()
            cur.execute(query)
            data = cur.fetchall()
            con.commit()
            cur.close()
            return data
        else:
            cur = con.cursor()
            cur.execute(query)
            con.commit()
            cur.close()
    else:
        cur = con.cursor()
        cur.execute(query, args)
        data = cur.fetchall()
        con.commit()
        cur.close()
        return data


def create_tables(con):
    """Функція для створення таблиць users, status, tasks"""

    commands = (
        """CREATE TABLE IF NOT EXISTS users (
        id SERIAL PRIMARY KEY,
        fullname VARCHAR(100) NOT NULL,
        email VARCHAR(100) UNIQUE 
        )""",

        """CREATE TABLE IF NOT EXISTS status (
        id SERIAL PRIMARY KEY,
        name VARCHAR(50) UNIQUE NOT NULL
        )""",

        """CREATE TABLE IF NOT EXISTS tasks (
        id SERIAL PRIMARY KEY,
        title VARCHAR(100),
        description TEXT,
        user_id INT,
        status_id INT,
        FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
        FOREIGN KEY (status_id) REFERENCES status(id) ON DELETE SET NULL ON UPDATE CASCADE
        )"""
    )

    cur = con.cursor()
    for command in commands:
        cur.execute(command)
    con.commit()
    cur.close()


def insert_users(con, usrs):
    """Функція для додавання записів до таблиці users"""

    sql = """INSERT INTO users (fullname, email) VALUES (%s, %s)"""   #id не нужно - должно ставится автоматически
    cur_execmany(con, sql, usrs)


def insert_status(con, sts):
    """Функція для додавання записів до таблиці status"""

    sql = """INSERT INTO status(name) VALUES (%s)"""   #id не нужно - должно ставится автоматически
    cur_execmany(con, sql, sts)

def insert_tasks(con, tsk):
    """Функція для додавання записів до таблиці tasks"""

    sql = """INSERT INTO tasks (title, description, user_id, status_id) VALUES (%s, %s, %s, %s)"""  
                              #id не нужно - должно ставится автоматически
    cur_execmany(con, sql, tsk)

def change_task_status(new_status, user_id, con):
    """Функція для зміни статусу завдання"""
    sql = """UPDATE tasks
    SET status_id = %s
    WHERE user_id = %s"""
    
    cur = con.cursor()
    try:
        cur.execute(sql, (new_status, user_id))
        print('Task status updated')
    except Exception as e:
        print(e)
    finally:
        con.commit()
        cur.close()


def update_username(new_uname, user_id, con):
    """Функція для зміни імені користувача"""
    
    sql = """UPDATE users
    SET fullname = %s
    WHERE id = %s"""
    
    cur = con.cursor()
    try:
        cur.execute(sql, (new_uname,user_id))
        print('User name updated')
    except Exception as e:
        print(e)
    finally:
        con.commit()
        cur.close()


def delete_task(task_id, con):
    """Функція для видалення завдання за його id."""
    
    sql = """DELETE FROM tasks WHERE id = %s"""
    cur = con.cursor()
    cur.execute(sql, (task_id,))
    print("Task deleted")
    con.commit()
    cur.close()


def select_user_tasks(username, con):
    """Функція для отримання завдань певного користувача за його user_id."""

    sql = """SELECT t.title, t.description, st.name
            FROM users us LEFT JOIN tasks t ON us.id = t.user_id
            LEFT JOIN status st ON t.status_id = st.id
            WHERE us.fullname = %s """
    return cur_exec(con, sql, username)


def select_task_by_status(status, con):
    """Функція для вибору завдань з певния статусом"""

    sql = """SELECT st.name, t.*, us.fullname
            FROM tasks t LEFT JOIN status st ON t.status_id = st.id
            LEFT JOIN users us ON t.user_id = us.id
            WHERE st.name = %s """
    return cur_exec(con, sql, status)


def select_other_status(status_name, con):
    """Функція для вибору завдань, чий статус не є 'завершено'"""

    sql = """SELECT st.name, t.*, us.fullname
            FROM tasks t LEFT JOIN status st ON t.status_id = st.id
            LEFT JOIN users us ON t.user_id = us.id
            WHERE st.name != %s """
    return cur_exec(con, sql, status_name)


def select_users_no_tasks(con):
    """Функція для отримання списку користувачів, які не мають жодного завдання, з використанням 
    комбінації SELECT, WHERE NOT IN і підзапиту."""

    sql = """SELECT us.id, us.fullname, t.id
            FROM users us LEFT JOIN tasks t
            ON us.id = t.user_id
            WHERE us.id NOT IN (SELECT t.user_id FROM tasks t)
    """
    return cur_exec(con, sql)


def find_user_email(email_to_search, con):
    """Функція для знаходження користувачів з певною електронною поштою"""

    sql = """SELECT *
            FROM users 
            WHERE email LIKE '%%' || %s
    """
    return cur_exec(con, sql, email_to_search)


def find_task_email(email_to_search, con):
    """Функція для знаходження завдань, які призначені користувачам з певною електронною адресою"""
    sql = """SELECT us.id, us.email, us.fullname, t.title, t.description
            FROM users us LEFT JOIN tasks t
            ON us.id = t.user_id
            WHERE us.email LIKE '%%' || %s || '%%'
    """
    return cur_exec(con, sql, email_to_search)


def count_tasks_by_status(con):
    """Функція для отримання кількості завдань для кожного статусу"""

    sql = """SELECT st.name, COUNT(t.status_id)
    FROM tasks t LEFT JOIN status st ON t.status_id = st.id
    GROUP BY st.name
    """
    return cur_exec(con, sql)
     

def find_empty_tasks(con):
    """Функція для отримання списку завдань, що не мають опису"""

    sql = """SELECT * FROM tasks
    WHERE description is NULL"""
    return cur_exec(con, sql)


def get_users_with_status(con, status_name):
    """Функція для вибору користувачів та їхніх завдань у певному статусі (наприклад, 'in progress')"""

    sql = """SELECT us.fullname, t.id, t.title, st.name
    FROM users us INNER JOIN tasks t ON us.id = t.user_id
    LEFT JOIN status st ON t.status_id = st.id
    WHERE st.name = %s"""
    return cur_exec(con, sql, status_name)
 

def count_tasks_per_user(con):
    """Функція для підрахунку кількості завдань кожного користувача"""

    sql = """SELECT us.id, us.fullname, COUNT(t.user_id)
    FROM users us LEFT JOIN tasks t on us.id = t.user_id
    GROUP BY us.id, us.fullname"""
    return cur_exec(con, sql)
     

if __name__ == "__main__":
    
    #підключаємось до БД
    conn = connect_db(base_configs, configs)

    #додаємо згенеровані дані для users, statuses та tasks до відповідних таблиць
    users = generate_users()
    statuses = generate_status() 
    tasks = generate_tasks(TASKS, users) 
    create_tables(conn)
    insert_users(conn, users)
    insert_status(conn, statuses)
    insert_tasks(conn, tasks)

    #вивести всі завдання вказаного користувача
    u_name = 'Франчук Михайло Азарович'
    print(f.CYAN + f'SELECTED TASKS FOR {u_name}' + f.RESET)
    for record in select_user_tasks(u_name, conn):
        print(record)
    
    #вивести всі завдання у статусі 'new'
    st = 'new'
    print(f.CYAN + f'ALL TASKS WITH STATUS {st}'+ f.RESET)
    for record in select_task_by_status(st, conn):
        print(record)

    #вивести користувачів, які не мають завдань
    print(f.CYAN + 'USERS WITH NO TASKS' + f.RESET)
    for record in select_users_no_tasks(conn):
        print(record)

    #вивести всі завдання, які не є 'completed'
    completed_status = "completed"
    print(f.CYAN + "ALL NOT 'COMPLETED' TASKS" + f.RESET)       
    for record in select_other_status(completed_status, conn):
        print(record)

    #вивести список користувачів з електронною адресою 'gov.ua'
    email_pattern = 'gov.ua'
    print(f.CYAN + f"FIND ALL '{email_pattern}' EMAILS" + f.RESET)
    for record in find_user_email(email_pattern, conn):
        print(record)
    
    #вивести завдання користувачів з електронною адресою 'gmail.com'
    e_pattern = 'gmail.com'
    print(f.CYAN + f"FIND TASKS ASSIGNED TO '{e_pattern}' EMAIL" + f.RESET)
    for record in find_task_email(e_pattern, conn):
        print(record)

    #вивести завдання, які не мають опису
    print(f.CYAN + "TASKS WITH NO DESCRIPTION" + f.RESET) 
    for t in find_empty_tasks(conn):
        print(t)
    
    #вивести список користувачів, які мають завдання у статусі 'in progress'
    status_name = 'in progress'
    print(f.CYAN + f"USERS WITH TASKS IN STATUS {status_name}" + f.RESET)
    for record in get_users_with_status(conn, status_name):
        print(record)
    
    #змінити статус завдання з 'new' на 'in progress'
    print(f.CYAN + f"CHANGED STATUS FROM 'new' TO 'in progress'" + f.RESET)
    change_task_status(2, 6, conn) 

    #змінити ім'я (fullname) користувача з id '11' (fullname: 'Павлик Охрім Яремович') на 'Павленко Охрім Яремович'
    new_username = 'Павленко Охрім Яремович'
    user_id = 11
    print(f.CYAN + f"CHANGED USERNAME TO {new_username} FOR USER_ID {user_id}" + f.RESET)
    update_username(new_username, user_id, conn)

    #додати нове завдання до таблиці tasks
    new_task = [('System Performance Monitoring', "Analyze system performance metrics for the past month", None, 1),]
    print(f.CYAN + "NEW TASK ADDED TO TASKS" + f.RESET)
    insert_tasks(conn, new_task)

    #вивести кількість завдань кожного статусу
    print(f.CYAN + "COUNT TASKS BY STATUS" + f.RESET)
    print(count_tasks_by_status(conn))

    #вивести кількість завдань кожного користувача
    print(f.CYAN + "COUNT USERS TASKS" + f.RESET)
    for record in count_tasks_per_user(conn):
        print(record)

    #видалити завдання з id '19'
    task_id_to_delete = 19
    print(f.CYAN + "TASK DELETED" + f.RESET)
    delete_task(task_id_to_delete, conn)

    conn.close()