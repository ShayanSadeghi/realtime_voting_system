import psycopg2

conn = psycopg2.connect("host=localhost dbname=voting user=postgres password=postgres")
cur = conn.cursor()

CANDIDATES = ["candidate-1", "candidate-2", "candidate-3", "candidate-4"]

NUM_USERS = 1000


def create_pg_dbs():

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS candidates (
            candidate_id SERIAL PRIMARY KEY,
            candidate_name VARCHAR(255) NOT NULL
        )
    """
    )

    cur.execute(
        """
            CREATE TABLE IF NOT EXISTS voters (
                user_id SERIAL PRIMARY KEY,
                user_name VARCHAR(255),
                national_code CHAR(10) CHECK (national_code ~ '^[0-9]{10}$'),
                phone CHAR(11) CHECK (phone ~ '^0[0-9]{10}$')
            )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS voters_history (
        vote_id SERIAL PRIMARY KEY,
        user_id INTEGER REFERENCES voters(user_id),
        candidate_id INTEGER REFERENCES candidates(candidate_id),
        ts  TIMESTAMP
        )
    """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS vote_results (
            candidate_id INTEGER,
            votes INTEGER
        )
    """
    )
    conn.commit()


if __name__ == "__main__":
    create_pg_dbs()
    cur.execute("SELECT * FROM voters")
    users = cur.fetchall()
    for c in CANDIDATES:
        cur.execute("INSERT INTO candidates(candidate_name) VALUES(%s)", (c,))

    for i in range(1, NUM_USERS + 1):
        national_code = f'012345{i:04d}'
        phone_number = f'0901234{i:04d}'
        cur.execute("INSERT INTO voters(user_name, national_code, phone) VALUES(%s, %s, %s)", (f"user_{i}", national_code, phone_number))

    conn.commit()
