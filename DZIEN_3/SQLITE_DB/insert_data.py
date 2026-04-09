import sqlite3
from sqlite3 import Error
from conn import create_connection

def create_project(conn,project):
    sql = """
        INSERT INTO projects(name,begin_date,end_date) VALUES(?,?,?)
    """

    cur = conn.cursor()
    cur.execute(sql,project)
    conn.commit()
    return cur.lastrowid


def create_task(conn, task):
    sql = """
        INSERT INTO tasks(name,priority,status_id,project_id,begin_date,end_date) VALUES(?,?,?,?,?,?)
    """

    cur = conn.cursor()
    cur.execute(sql, task)
    conn.commit()
    return cur.lastrowid

def insert():
    database = r"sql\mojabaza12.db"
    conn = create_connection(database)

    with conn:
        project1 = ('Super Apka -> Python Data','2023-12-20','2024-02-10')
        project_id_1 = create_project(conn,project1)

        project2 = ('Siec neuronowa -> Python', '2023-12-28', '2024-06-12')
        project_id_2 = create_project(conn, project2)

        task_11 = ("Analiza wymagań dotyczących aplikacji",1,1,project_id_1,'2023-12-27','2023-12-30')
        task_12 = ("Przygotowanie diagraamów UML",2,1,project_id_1,'2024-01-03','2024-01-15')

        task_21 = ("Analiza wymagań dotyczących aplikacji", 1, 1, project_id_2, '2024-01-02', '2024-01-27')
        task_22 = ("Przygotowanie modelu sieci neuronowej", 3, 1, project_id_2, '2024-02-02', '2024-02-16')


        create_task(conn,task_11)
        create_task(conn,task_12)
        create_task(conn,task_21)
        create_task(conn,task_22)
