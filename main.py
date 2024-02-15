import sqlalchemy as sq
import configparser

from sqlalchemy.orm import sessionmaker
import json
from models import Publisher, Book, Stock, Shop, Sale, create_tables


config = configparser.ConfigParser()
config.read('config.ini')
user = config['params']['user']
password = config['params']['password']
host = config['params']['host']
port = config['params']['port']
base_name = config['params'][base_name]

DSN = f'postgresql://{user}:{password}@{host}:{port}/{base_name}'
engine = sq.create_engine(DSN)
Session = sessionmaker(bind=engine)
session = Session()

create_tables(engine)


Session = sessionmaker(bind=engine)
session = Session()

def insert_data():
    with session as sn:
        with open('tests_data.json', 'r') as fd:
            data = json.load(fd)

        for record in data:
            model = {
                'publisher': Publisher,
                'shop': Shop,
                'book': Book,
                'stock': Stock,
                'sale': Sale,
            }[record.get('model')]
            session.add(model(id=record.get('pk'), **record.get('fields')))
        session.commit()

def get_info(publisher=input('Please enter Publisher name or her ID: ')):
    publisher_query = session.query(Publisher.name, Publisher.id).all()
    publisher_list = {}

    for item in publisher_query:
        publisher_list.setdefault(item[1], item[0])

    if publisher.isdigit() and int(publisher) <= len(publisher_list):

        counter = 0
        for q in session.query(Book.title, Shop.name, Sale.price, Sale.date_sale).\
                join(Publisher).join(Stock).join(Shop).join(Sale).\
                filter(Book.id_publisher == publisher).all():
            for title in q:
                if counter < 3:
                    print(f'{title} | ', end='')
                    counter += 1
                else:
                    print(f'{title}', end='\n')
                    counter = 0

    elif publisher and publisher is publisher_list.values():

        counter = 0
        for q in (session.query(Book.title, Shop.name, Sale.price, Sale.date_sale).\
                join(Publisher).join(Stock).join(Shop).join(Sale).\
                filter(Publisher.name == publisher).all()):
            for title in q:
                if counter < 3:
                    print(f'{title} | ', end='')
                    counter += 1
                else:
                    print(f'{title}', end='\n')
                    counter = 0
    else:
        print('You have input a wrong data.\nYou need to input just Publisher name or his ID.\nTry again!')
        get_info(input())

insert_data()
get_info()

