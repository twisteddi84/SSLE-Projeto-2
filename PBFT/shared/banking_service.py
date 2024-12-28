import sqlite3

class BankingService:
    def __init__(self, db_name):
        self.conn = sqlite3.connect(db_name, check_same_thread=False)
        self.create_table()

    def create_table(self):
        with self.conn:
            self.conn.execute("""
            CREATE TABLE IF NOT EXISTS accounts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                balance REAL NOT NULL DEFAULT 0.0
            )
            """)

    def create_account(self, name, initial_balance):
        name = str(name)  # Ensure name is a string
        initial_balance = float(initial_balance)  # Ensure balance is a float
        print(f"[DEBUG] Creating account with name={name}, balance={initial_balance}")
        self.conn.execute("INSERT INTO accounts (name, balance) VALUES (?, ?)", (name, initial_balance))
        self.conn.commit()

    def deposit(self, name, amount):
        with self.conn:
            self.conn.execute("UPDATE accounts SET balance = balance + ? WHERE name = ?", (amount, name))
        print(f"Deposited {amount} into {name}'s account.")

    def withdraw(self, name, amount):
        with self.conn:
            cursor = self.conn.execute("SELECT balance FROM accounts WHERE name = ?", (name,))
            row = cursor.fetchone()
            if row and row[0] >= amount:
                self.conn.execute("UPDATE accounts SET balance = balance - ? WHERE name = ?", (amount, name))
                print(f"Withdrew {amount} from {name}'s account.")
            else:
                print("Insufficient funds or account not found.")

    def get_balance(self, name):
        cursor = self.conn.execute("SELECT balance FROM accounts WHERE name = ?", (name,))
        row = cursor.fetchone()
        return row[0] if row else None
