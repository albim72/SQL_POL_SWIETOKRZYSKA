import sqlite3
from sqlite3 import Error
from conn import create_connection

def select_all_tasks(conn):
    cur = conn.cursor()
    cur.execute("SELECT * FROM tasks")
    
    rows = cur.fetchall()
    for row in rows:
        print(row)


def select_all_projects(conn):
    cur = conn.cursor()
    cur.execute("SELECT * FROM projects")

    rows = cur.fetchall()
    for row in rows:
        print(row)
        
def select_task_by_priority(conn,priority):
    cur = conn.cursor()
    cur.execute("SELECT * FROM tasks WHERE priority = ?",(priority,))

    rows = cur.fetchall()
    for row in rows:
        print(row)
        
def select():
    database = r"sql\mojabaza12.db"
    conn = create_connection(database)
    
    with conn:
        print("1. Zadanie z filtrem po priorytecie")
        prio = input("podaj poriorytet zadania: ")
        select_task_by_priority(conn,prio)
        
        print("2.Wszystkie zadania: ")
        select_all_tasks(conn)

        print("2.Wszystkie projekty: ")
        select_all_projects(conn)
