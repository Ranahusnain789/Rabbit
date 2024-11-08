    import sqlite3

    def reset_users_db():
        conn = sqlite3.connect('users.db')
        cursor = conn.cursor()
        cursor.execute('DROP TABLE IF EXISTS users')
        cursor.execute('''
            CREATE TABLE users (
                username TEXT PRIMARY KEY,
                password TEXT NOT NULL,
                location TEXT,
                is_admin BOOLEAN NOT NULL CHECK (is_admin IN (0, 1))
            )
        ''')
        conn.commit()
        conn.close()
        print("users.db has been reset.")

    def reset_user_mapping_db():
        conn = sqlite3.connect('user_mapping.db')
        cursor = conn.cursor()
        cursor.execute('DROP TABLE IF EXISTS mapping')
        cursor.execute('''
            CREATE TABLE mapping (
                user_id TEXT PRIMARY KEY,
                mapped_data TEXT
            )
        ''')
        conn.commit()
        conn.close()
        print("user_mapping.db has been reset.")
