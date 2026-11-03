import random
import time
import mysql.connector
from faker import Faker

fake = Faker()
conn = mysql.connector.connect(host='localhost', port=3306, user='root',
                               password='root', database='shop', autocommit=True)
cur = conn.cursor()


def random_id(table):
    cur.execute('SELECT id FROM ' + table + ' ORDER BY RAND() LIMIT 1')
    row = cur.fetchone()
    return row[0] if row else None


def new_customer():
    cur.execute('INSERT INTO customers (name, email, city) VALUES (%s, %s, %s)',
                (fake.name(), fake.unique.email(), fake.city()))


def seed():
    cur.execute('SELECT COUNT(*) FROM customers')
    if cur.fetchone()[0] == 0:
        for _ in range(30):
            new_customer()
        items = [('Laptop', 'Electronics'), ('Phone', 'Electronics'), ('Headphones', 'Electronics'),
                 ('Desk', 'Furniture'), ('Chair', 'Furniture'), ('Notebook', 'Stationery'),
                 ('Pen Set', 'Stationery'), ('Backpack', 'Accessories'), ('Watch', 'Accessories'),
                 ('Lamp', 'Furniture')]
        for name, category in items:
            cur.execute('INSERT INTO products (name, category, price) VALUES (%s, %s, %s)',
                        (name, category, round(random.uniform(5, 500), 2)))


def new_order():
    cid, pid = random_id('customers'), random_id('products')
    cur.execute('SELECT price FROM products WHERE id = %s', (pid,))
    price = float(cur.fetchone()[0])
    qty = random.randint(1, 5)
    cur.execute('INSERT INTO orders (customer_id, product_id, quantity, status, order_total) '
                'VALUES (%s, %s, %s, %s, %s)', (cid, pid, qty, 'created', round(qty * price, 2)))


def update_order():
    oid = random_id('orders')
    if oid:
        cur.execute('UPDATE orders SET status = %s WHERE id = %s',
                    (random.choice(['paid', 'shipped', 'cancelled']), oid))


def delete_order():
    oid = random_id('orders')
    if oid:
        cur.execute('DELETE FROM orders WHERE id = %s', (oid,))


def update_customer():
    cid = random_id('customers')
    cur.execute('UPDATE customers SET city = %s WHERE id = %s', (fake.city(), cid))


seed()
actions = [new_order, update_order, delete_order, update_customer, new_customer]
weights = [55, 25, 8, 7, 5]
while True:
    action = random.choices(actions, weights)[0]
    action()
    print('did', action.__name__)
    time.sleep(random.uniform(0.3, 1.0))