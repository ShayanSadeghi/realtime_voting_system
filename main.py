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
            candidate_name VARCHAR(255)
        )
    """
    )

    cur.execute(
        """
            CREATE TABLE IF NOT EXISTS voters (
                user_id SERIAL PRIMARY KEY,
                user_name VARCHAR(255)
            )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS voters_history (
        id SERIAL PRIMARY KEY,
        user_id INTEGER,
        candidate_id INTEGER,
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
    print(users)
    for c in CANDIDATES:
        print(c)
        cur.execute("INSERT INTO candidates(candidate_name) VALUES(%s)", (c,))

    for i in range(1, NUM_USERS + 1):
        cur.execute("INSERT INTO voters(user_name) VALUES(%s)", (f"usser_{i}",))

    conn.commit()
