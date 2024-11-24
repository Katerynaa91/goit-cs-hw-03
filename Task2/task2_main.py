"""Завдання 2.
Розробіть Python скрипт, який використовує бібліотеку PyMongo 
для реалізації основних CRUD (Create, Read, Update, Delete) операцій у MongoDB.
Створіть базу даних відповідно до вимог. Кожен документ у базі даних представляє 
інформацію про кота, його ім'я name (type str), вік age (type int) та характеристики features (type list)."""
import os
from dotenv import load_dotenv
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from records_data import cats_data


def create_collection(collection_name, data):
    """Функція для створення колекції у базі даних"""
    try:
        collection_name.insert_many(data)
        print("Collection added")
    except Exception as e:
        print(e)
 
def show_all(collection_name):
    """Функція для виведення всіх записів із колекції (операція READ)"""
    return [record for record in collection_name.find({})]

def show_info_by_name(collection_name, cat_name):
    """Функція, яка дозволяє користувачеві ввести ім'я кота та виводить інформацію про цього кота (операція READ)"""
    get_info = collection_name.find_one({"name": cat_name})
    if get_info is not None:
        return get_info
    return "Record doesn not exist or entered name is not correct"

def update_age_by_name(collection_name, cat_name, new_age):
    """Функція, яка дозволяє дозволяє користувачеві оновити вік кота за ім'ям (операція Update)"""
    try:
        query_filter = {'name': cat_name}
        update_op = {'$set':{'age': new_age}}
        print(cat_name, "age updated")
        return collection_name.update_one(query_filter, update_op)
    except Exception as e:
        print(e)

def add_feature_by_name(collection_name, cat_name, new_feature):
    """Функція, яка дозволяє додати нову характеристику до списку features кота за ім'ям (операція Update)"""
    try:
        query_filter = {'name': cat_name}
        features_record = collection_name.find_one({'name': cat_name})['features']
        features_record.append(new_feature)
        update_op = {'$set':{'features': features_record}}
        print("new feature added for", cat_name)
        return collection_name.update_one(query_filter, update_op)
    except Exception as e:
        print(e)
 
def delete_by_name(collection_name, cat_name):
    """Функція для видалення запису з колекції за ім'ям тварини (операція Delete)"""
    try:
        query_filter = {'name': cat_name}
        print(f"Record for {cat_name} deleted")
        return collection_name.delete_one(query_filter)
    except Exception as e:
        print(e)

def delete_all(collection_name):
    """Функція для видалення всіх записів із колекції (операція Delete)"""
    try:
        print(f"""Collection '{collection_name.name}' has {collection_name.count_documents({})} documents. 
All records deleted""")
        return collection_name.delete_many({})
    except Exception as e:
        print(e)
        
        
if __name__ == "__main__":

    load_dotenv()
    p = os.getenv("PASS")

    uri = f"mongodb+srv://katerynaa:{p}@cats.q8uym.mongodb.net/?retryWrites=true&w=majority&appName=Cats"
    client = MongoClient(uri, server_api=ServerApi('1'))
    
    # #перевірка з'єднання з сервером MongoDB
    # try:
    #     client.admin.command('ping')
    #     print("Pinged your deployment. You successfully connected to MongoDB!")
    # except Exception as e:
    #     print(e)

    #створюємо БД та колекцію
    mydb = client['cats_db']
    collection = mydb['cats']

    create_collection(collection, cats_data)

    #перевірка функції для виведення всіх записів
    for record in show_all(collection):  
        print(record)

    #перевірка функції для виведення інформації про кота за введеним ім'ям
    ct_name = "murzik"
    for k, v in show_info_by_name(collection, ct_name).items():
        print(f"{k.upper()}: {v}")

    #перевірка функції для зміни віку кота за введеним ім'ям
    cat_name = 'boris'
    print(f"Age before update: {show_info_by_name(collection, cat_name)['age']}")
    update_age_by_name(collection, cat_name, 3)
    print(f"Age after update: {show_info_by_name(collection, cat_name)['age']}")

    #перевірка функції для додавання нової характеристики до списку features
    new_feature = 'любить дивитись на птахів'
    add_feature_by_name(collection, cat_name, new_feature)
    print(f"Features after update: {show_info_by_name(collection, cat_name)['features']}")

    #перевірка функції для видалення запису/документа про кота за введения ім'ям
    cat_to_delete = "pushok"
    delete_by_name(collection, cat_to_delete)

    #перевірка функції для видалення всіх записів з колекції
    delete_all(collection)
    
