"""
Sakila数据库初始化脚本
基于Sakila样本数据库创建SQLite版本，用于Text2SQL评估
"""

import sqlite3
import os
from datetime import datetime

def create_sakila_database(db_path: str = "90-文档-Data/sakila.db"):
    """
    创建Sakila数据库并插入示例数据
    
    Args:
        db_path: 数据库文件路径
    """
    
    # 确保目录存在
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    
    # 连接到SQLite数据库
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    print("🚀 开始创建Sakila数据库...")
    
    # 1. 创建actor表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS actor (
        actor_id INTEGER PRIMARY KEY AUTOINCREMENT,
        first_name TEXT NOT NULL,
        last_name TEXT NOT NULL,
        last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')
    
    # 2. 创建category表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS category (
        category_id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')
    
    # 3. 创建language表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS language (
        language_id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')
    
    # 4. 创建film表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS film (
        film_id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT NOT NULL,
        description TEXT,
        release_year INTEGER,
        language_id INTEGER NOT NULL,
        original_language_id INTEGER,
        rental_duration INTEGER DEFAULT 3,
        rental_rate DECIMAL(4,2) DEFAULT 4.99,
        length INTEGER,
        replacement_cost DECIMAL(5,2) DEFAULT 19.99,
        rating TEXT DEFAULT 'G',
        special_features TEXT,
        last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (language_id) REFERENCES language(language_id),
        FOREIGN KEY (original_language_id) REFERENCES language(language_id)
    )''')
    
    # 5. 创建film_actor关系表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS film_actor (
        actor_id INTEGER NOT NULL,
        film_id INTEGER NOT NULL,
        last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (actor_id, film_id),
        FOREIGN KEY (actor_id) REFERENCES actor(actor_id),
        FOREIGN KEY (film_id) REFERENCES film(film_id)
    )''')
    
    # 6. 创建film_category关系表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS film_category (
        film_id INTEGER NOT NULL,
        category_id INTEGER NOT NULL,
        last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (film_id, category_id),
        FOREIGN KEY (film_id) REFERENCES film(film_id),
        FOREIGN KEY (category_id) REFERENCES category(category_id)
    )''')
    
    # 7. 创建address表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS address (
        address_id INTEGER PRIMARY KEY AUTOINCREMENT,
        address TEXT NOT NULL,
        address2 TEXT,
        district TEXT NOT NULL,
        city_id INTEGER NOT NULL,
        postal_code TEXT,
        phone TEXT NOT NULL,
        last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')
    
    # 8. 创建city表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS city (
        city_id INTEGER PRIMARY KEY AUTOINCREMENT,
        city TEXT NOT NULL,
        country_id INTEGER NOT NULL,
        last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')
    
    # 9. 创建country表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS country (
        country_id INTEGER PRIMARY KEY AUTOINCREMENT,
        country TEXT NOT NULL,
        last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')
    
    # 10. 创建store表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS store (
        store_id INTEGER PRIMARY KEY AUTOINCREMENT,
        manager_staff_id INTEGER NOT NULL,
        address_id INTEGER NOT NULL,
        last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (address_id) REFERENCES address(address_id)
    )''')
    
    # 11. 创建staff表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS staff (
        staff_id INTEGER PRIMARY KEY AUTOINCREMENT,
        first_name TEXT NOT NULL,
        last_name TEXT NOT NULL,
        address_id INTEGER NOT NULL,
        picture BLOB,
        email TEXT,
        store_id INTEGER NOT NULL,
        active INTEGER DEFAULT 1,
        username TEXT NOT NULL,
        password TEXT,
        last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (address_id) REFERENCES address(address_id),
        FOREIGN KEY (store_id) REFERENCES store(store_id)
    )''')
    
    # 12. 创建customer表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS customer (
        customer_id INTEGER PRIMARY KEY AUTOINCREMENT,
        store_id INTEGER NOT NULL,
        first_name TEXT NOT NULL,
        last_name TEXT NOT NULL,
        email TEXT,
        address_id INTEGER NOT NULL,
        active INTEGER DEFAULT 1,
        create_date DATETIME NOT NULL,
        last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (store_id) REFERENCES store(store_id),
        FOREIGN KEY (address_id) REFERENCES address(address_id)
    )''')
    
    # 13. 创建inventory表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS inventory (
        inventory_id INTEGER PRIMARY KEY AUTOINCREMENT,
        film_id INTEGER NOT NULL,
        store_id INTEGER NOT NULL,
        last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (film_id) REFERENCES film(film_id),
        FOREIGN KEY (store_id) REFERENCES store(store_id)
    )''')
    
    # 14. 创建rental表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS rental (
        rental_id INTEGER PRIMARY KEY AUTOINCREMENT,
        rental_date DATETIME NOT NULL,
        inventory_id INTEGER NOT NULL,
        customer_id INTEGER NOT NULL,
        return_date DATETIME,
        staff_id INTEGER NOT NULL,
        last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (inventory_id) REFERENCES inventory(inventory_id),
        FOREIGN KEY (customer_id) REFERENCES customer(customer_id),
        FOREIGN KEY (staff_id) REFERENCES staff(staff_id)
    )''')
    
    # 15. 创建payment表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS payment (
        payment_id INTEGER PRIMARY KEY AUTOINCREMENT,
        customer_id INTEGER NOT NULL,
        staff_id INTEGER NOT NULL,
        rental_id INTEGER,
        amount DECIMAL(5,2) NOT NULL,
        payment_date DATETIME NOT NULL,
        last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (customer_id) REFERENCES customer(customer_id),
        FOREIGN KEY (staff_id) REFERENCES staff(staff_id),
        FOREIGN KEY (rental_id) REFERENCES rental(rental_id)
    )''')
    
    print("✅ 数据库表结构创建完成")
    
    # 插入示例数据
    print("📊 开始插入示例数据...")
    
    # 插入语言数据
    languages = [
        (1, 'English'),
        (2, 'Italian'),
        (3, 'Japanese'),
        (4, 'Mandarin'),
        (5, 'French'),
        (6, 'German')
    ]
    cursor.executemany('INSERT OR REPLACE INTO language (language_id, name) VALUES (?, ?)', languages)
    
    # 插入类别数据
    categories = [
        (1, 'Action'),
        (2, 'Animation'),
        (3, 'Children'),
        (4, 'Classics'),
        (5, 'Comedy'),
        (6, 'Documentary'),
        (7, 'Drama'),
        (8, 'Family'),
        (9, 'Foreign'),
        (10, 'Games'),
        (11, 'Horror'),
        (12, 'Music'),
        (13, 'New'),
        (14, 'Sci-Fi'),
        (15, 'Sports'),
        (16, 'Travel')
    ]
    cursor.executemany('INSERT OR REPLACE INTO category (category_id, name) VALUES (?, ?)', categories)
    
    # 插入演员数据
    actors = [
        (1, 'PENELOPE', 'GUINESS'),
        (2, 'NICK', 'WAHLBERG'),
        (3, 'ED', 'CHASE'),
        (4, 'JENNIFER', 'DAVIS'),
        (5, 'JOHNNY', 'LOLLOBRIGIDA'),
        (6, 'BETTE', 'NICHOLSON'),
        (7, 'GRACE', 'MOSTEL'),
        (8, 'MATTHEW', 'JOHANSSON'),
        (9, 'JOE', 'SWANK'),
        (10, 'CHRISTIAN', 'GABLE'),
        (11, 'ZERO', 'CAGE'),
        (12, 'KARL', 'BERRY'),
        (13, 'UMA', 'WOOD'),
        (14, 'VIVIEN', 'BERGEN'),
        (15, 'CUBA', 'OLIVIER'),
        (16, 'FRED', 'COSTNER'),
        (17, 'HELEN', 'VOIGHT'),
        (18, 'DAN', 'TORN'),
        (19, 'BOB', 'FAWCETT'),
        (20, 'LUCILLE', 'TRACY')
    ]
    cursor.executemany('INSERT OR REPLACE INTO actor (actor_id, first_name, last_name) VALUES (?, ?, ?)', actors)
    
    # 插入电影数据
    films = [
        (1, 'ACADEMY DINOSAUR', 'A Epic Drama of a Feminist And a Mad Scientist who must Battle a Teacher in The Canadian Rockies', 2006, 1, None, 6, 0.99, 86, 20.99, 'PG', 'Deleted Scenes,Behind the Scenes'),
        (2, 'ACE GOLDFINGER', 'A Astounding Epistle of a Database Administrator And a Explorer who must Find a Car in Ancient China', 2006, 1, None, 3, 4.99, 48, 12.99, 'G', 'Trailers,Deleted Scenes'),
        (3, 'ADAPTATION HOLES', 'A Astounding Reflection of a Lumberjack And a Car who must Sink a Lumberjack in A Baloon Factory', 2006, 1, None, 7, 2.99, 50, 18.99, 'NC-17', 'Trailers,Deleted Scenes'),
        (4, 'AFFAIR PREJUDICE', 'A Fanciful Documentary of a Frisbee And a Lumberjack who must Chase a Monkey in A Shark Tank', 2006, 1, None, 5, 2.99, 117, 26.99, 'G', 'Commentaries,Behind the Scenes'),
        (5, 'AFRICAN EGG', 'A Fast-Paced Documentary of a Pastry Chef And a Dentist who must Pursue a Forensic Psychologist in The Gulf of Mexico', 2006, 1, None, 6, 2.99, 130, 22.99, 'G', 'Deleted Scenes'),
        (6, 'AGENT TRUMAN', 'A Intrepid Panorama of a Robot And a Boy who must Escape a Sumo Wrestler in Ancient China', 2006, 1, None, 3, 2.99, 169, 17.99, 'PG', 'Deleted Scenes'),
        (7, 'AIRPLANE SIERRA', 'A Touching Saga of a Hunter And a Butler who must Discover a Butler in A Jet Boat', 2006, 1, None, 6, 4.99, 62, 28.99, 'PG-13', 'Trailers,Deleted Scenes'),
        (8, 'AIRPORT POLLOCK', 'A Epic Tale of a Moose And a Girl who must Confront a Monkey in Ancient India', 2006, 1, None, 6, 4.99, 54, 15.99, 'R', 'Trailers'),
        (9, 'ALABAMA DEVIL', 'A Thoughtful Panorama of a Database Administrator And a Mad Scientist who must Outgun a Mad Scientist in A Jet Boat', 2006, 1, None, 3, 2.99, 114, 21.99, 'PG-13', 'Trailers,Deleted Scenes'),
        (10, 'ALADDIN CALENDAR', 'A Action-Packed Tale of a Man And a Lumberjack who must Reach a Feminist in Ancient China', 2006, 1, None, 6, 4.99, 63, 24.99, 'NC-17', 'Trailers,Deleted Scenes')
    ]
    cursor.executemany('INSERT OR REPLACE INTO film (film_id, title, description, release_year, language_id, original_language_id, rental_duration, rental_rate, length, replacement_cost, rating, special_features) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', films)
    
    # 插入film_actor关系数据
    film_actors = [
        (1, 1), (1, 23), (1, 25), (1, 106), (1, 140), (1, 166), (1, 277), (1, 361), (1, 438), (1, 499), (1, 506), (1, 509), (1, 605), (1, 635), (1, 749), (1, 832), (1, 939), (1, 970), (1, 980),
        (2, 3), (2, 31), (2, 47), (2, 105), (2, 130), (2, 382), (2, 384), (2, 392), (2, 414), (2, 453), (2, 485), (2, 532), (2, 589), (2, 612), (2, 650), (2, 665), (2, 687), (2, 730), (2, 732), (2, 811), (2, 817), (2, 841), (2, 865), (2, 873), (2, 889), (2, 903), (2, 926), (2, 964), (2, 974),
        (3, 17), (3, 40), (3, 42), (3, 87), (3, 111), (3, 185), (3, 289), (3, 329), (3, 363), (3, 394), (3, 396), (3, 446), (3, 449), (3, 464), (3, 478), (3, 506), (3, 561), (3, 742), (3, 754), (3, 881), (3, 957)
    ]
    cursor.executemany('INSERT OR REPLACE INTO film_actor (actor_id, film_id) VALUES (?, ?)', film_actors)
    
    # 插入film_category关系数据
    film_categories = [
        (1, 6), (2, 11), (3, 6), (4, 11), (5, 8), (6, 9), (7, 5), (8, 11), (9, 4), (10, 7)
    ]
    cursor.executemany('INSERT OR REPLACE INTO film_category (film_id, category_id) VALUES (?, ?)', film_categories)
    
    # 插入国家数据
    countries = [
        (1, 'Afghanistan'), (2, 'Algeria'), (3, 'American Samoa'), (4, 'Angola'), (5, 'Anguilla'),
        (6, 'Argentina'), (7, 'Armenia'), (8, 'Australia'), (9, 'Austria'), (10, 'Azerbaijan'),
        (103, 'United States')
    ]
    cursor.executemany('INSERT OR REPLACE INTO country (country_id, country) VALUES (?, ?)', countries)
    
    # 插入城市数据
    cities = [
        (1, 'A Corua (La Corua)', 87), (2, 'Abha', 82), (3, 'Abu Dhabi', 101), (4, 'Acua', 60),
        (5, 'Adana', 97), (6, 'Addis Abeba', 31), (7, 'Aden', 107), (8, 'Adoni', 44),
        (9, 'Ahmadnagar', 44), (10, 'Akishima', 50), (300, 'Akron', 103), (312, 'Aurora', 103)
    ]
    cursor.executemany('INSERT OR REPLACE INTO city (city_id, city, country_id) VALUES (?, ?, ?)', cities)
    
    # 插入地址数据
    addresses = [
        (1, '47 MySakila Drive', None, 'Alberta', 300, '', ''), 
        (2, '28 MySQL Boulevard', None, 'QLD', 576, '', ''),
        (3, '23 Workhaven Lane', None, 'Alberta', 300, '', ''),
        (4, '1411 Lillydale Drive', None, 'QLD', 576, '', ''),
        (5, '1913 Hanoi Way', 'Suite 1', 'Nagasaki', 463, '35200', '28303384290')
    ]
    cursor.executemany('INSERT OR REPLACE INTO address (address_id, address, address2, district, city_id, postal_code, phone) VALUES (?, ?, ?, ?, ?, ?, ?)', addresses)
    
    # 插入商店数据
    stores = [
        (1, 1, 1),
        (2, 2, 2)
    ]
    cursor.executemany('INSERT OR REPLACE INTO store (store_id, manager_staff_id, address_id) VALUES (?, ?, ?)', stores)
    
    # 插入员工数据
    staff_members = [
        (1, 'Mike', 'Hillyer', 3, None, 'Mike.Hillyer@sakilastaff.com', 1, 1, 'Mike', '8cb2237d0679ca88db6464eac60da96345513964'),
        (2, 'Jon', 'Stephens', 4, None, 'Jon.Stephens@sakilastaff.com', 2, 1, 'Jon', None)
    ]
    cursor.executemany('INSERT OR REPLACE INTO staff (staff_id, first_name, last_name, address_id, picture, email, store_id, active, username, password) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', staff_members)
    
    # 插入客户数据
    customers = [
        (1, 1, 'MARY', 'SMITH', 'MARY.SMITH@sakilacustomer.org', 5, 1, '2006-02-14 22:04:36'),
        (2, 1, 'PATRICIA', 'JOHNSON', 'PATRICIA.JOHNSON@sakilacustomer.org', 6, 1, '2006-02-14 22:04:36'),
        (3, 1, 'LINDA', 'WILLIAMS', 'LINDA.WILLIAMS@sakilacustomer.org', 7, 1, '2006-02-14 22:04:36'),
        (4, 2, 'BARBARA', 'JONES', 'BARBARA.JONES@sakilacustomer.org', 8, 1, '2006-02-14 22:04:36'),
        (5, 1, 'ELIZABETH', 'BROWN', 'ELIZABETH.BROWN@sakilacustomer.org', 9, 1, '2006-02-14 22:04:36')
    ]
    cursor.executemany('INSERT OR REPLACE INTO customer (customer_id, store_id, first_name, last_name, email, address_id, active, create_date) VALUES (?, ?, ?, ?, ?, ?, ?, ?)', customers)
    
    # 插入库存数据
    inventories = [
        (1, 1, 1), (2, 1, 1), (3, 1, 1), (4, 1, 1), (5, 1, 2),
        (6, 2, 1), (7, 2, 1), (8, 2, 2), (9, 2, 2), (10, 3, 1),
        (11, 3, 1), (12, 3, 2), (13, 4, 1), (14, 4, 1), (15, 4, 2),
        (16, 5, 1), (17, 5, 1), (18, 5, 2), (19, 6, 1), (20, 6, 2)
    ]
    cursor.executemany('INSERT OR REPLACE INTO inventory (inventory_id, film_id, store_id) VALUES (?, ?, ?)', inventories)
    
    # 插入租赁数据
    rentals = [
        (1, '2005-05-24 22:53:30', 367, 130, '2005-05-26 22:04:30', 1),
        (2, '2005-05-24 22:54:33', 1525, 459, '2005-05-28 19:40:33', 1),
        (3, '2005-05-24 23:03:39', 1711, 408, '2005-06-01 22:12:39', 1),
        (4, '2005-05-24 23:04:41', 2452, 333, '2005-06-03 01:43:41', 2),
        (5, '2005-05-24 23:05:21', 2079, 222, '2005-06-02 04:33:21', 1)
    ]
    cursor.executemany('INSERT OR REPLACE INTO rental (rental_id, rental_date, inventory_id, customer_id, return_date, staff_id) VALUES (?, ?, ?, ?, ?, ?)', rentals)
    
    # 插入支付数据
    payments = [
        (1, 1, 1, 76, 2.99, '2007-01-24 21:40:19.996577'),
        (2, 1, 1, 573, 0.99, '2007-01-25 15:16:50.996577'),
        (3, 1, 1, 1185, 5.99, '2007-01-26 08:46:53.996577'),
        (4, 1, 2, 1422, 0.99, '2007-01-26 21:01:57.996577'),
        (5, 1, 2, 1476, 9.99, '2007-01-27 00:01:21.996577')
    ]
    cursor.executemany('INSERT OR REPLACE INTO payment (payment_id, customer_id, staff_id, rental_id, amount, payment_date) VALUES (?, ?, ?, ?, ?, ?)', payments)
    
    print("✅ 示例数据插入完成")
    
    # 提交更改并关闭连接
    conn.commit()
    conn.close()
    
    print(f"🎉 Sakila数据库创建完成！位置: {db_path}")
    
    return db_path

def verify_database(db_path: str):
    """
    验证数据库是否创建成功
    
    Args:
        db_path: 数据库文件路径
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    print("\n📊 数据库验证结果:")
    print("-" * 40)
    
    # 检查各表的记录数
    tables = [
        'actor', 'category', 'language', 'film', 'film_actor', 'film_category',
        'country', 'city', 'address', 'store', 'staff', 'customer', 
        'inventory', 'rental', 'payment'
    ]
    
    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        print(f"{table:15}: {count:4d} 记录")
    
    # 测试几个基本查询
    print("\n🔍 基本查询测试:")
    print("-" * 40)
    
    # 测试1: 获取所有演员
    cursor.execute("SELECT COUNT(*) FROM actor")
    actor_count = cursor.fetchone()[0]
    print(f"演员总数: {actor_count}")
    
    # 测试2: 获取所有电影
    cursor.execute("SELECT COUNT(*) FROM film")
    film_count = cursor.fetchone()[0]
    print(f"电影总数: {film_count}")
    
    # 测试3: 测试连接查询
    cursor.execute("SELECT COUNT(*) FROM film f JOIN film_category fc ON f.film_id = fc.film_id JOIN category c ON fc.category_id = c.category_id")
    join_count = cursor.fetchone()[0]
    print(f"电影-类别关联: {join_count}")
    
    conn.close()
    print("✅ 数据库验证完成")

def main():
    """主函数"""
    print("🚀 开始初始化Sakila数据库...")
    
    # 创建数据库
    db_path = create_sakila_database()
    
    # 验证数据库
    verify_database(db_path)
    
    print(f"\n🎉 Sakila数据库初始化完成！")
    print(f"数据库位置: {db_path}")
    print("现在可以运行Text2SQL评估系统了！")

if __name__ == "__main__":
    main() 