#Файл зі скиптами, що генерують фейкові дані для подальшого заповнення ними БД
import random
from faker import Faker
from tasks import TASKS

def generate_users(n=20):
    """Функція для генерації даних для таблиці users (для полів fullname та email)"""
    Faker.seed(42)
    f = Faker(locale='uk_UA')
    user_data = []
    for _ in range(1, n):
        user_data.append((f.full_name(), f.free_email()))
    return user_data

def generate_status(): 
    """Функція, яка повертає дані для таблиці status"""
    data =  [('new',), ('in progress',), ('completed',)]
    return data

def generate_tasks(tsks, user_data): 
    """Функція, яка створює та повертає дані для таблиці tasks"""
    random.seed(8)

    user_ids = [_ for _ in range(1, len(user_data))]
    random.shuffle(user_ids)

    status_id = [random.choice([_ for _ in range(1,4)]) for _ in range(len(tsks))]
    data_to_sql = []

    for (title, description), uid, sid in zip(tsks, user_ids[:len(tsks)], status_id):
        data_to_sql.append((title, description, uid, sid))

    return data_to_sql


print(generate_users(n=20))