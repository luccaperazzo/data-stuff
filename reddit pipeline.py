import praw
import pandas as pd
import sqlite3
from datetime import datetime

def configure_reddit(client_id, client_secret, user_agent):
    reddit = praw.Reddit(
        client_id=client_id,
        client_secret=client_secret,
        user_agent=user_agent
    )
    return reddit

def extract_posts(reddit, subreddit_name, limit=10):
    subreddit = reddit.subreddit(subreddit_name)
    posts = []
    for post in subreddit.hot(limit=limit):
        posts.append({
            "id": post.id,
            "title": post.title,
            "score": post.score,
            "author": str(post.author),
            "created_utc": datetime.utcfromtimestamp(post.created_utc),
            "num_comments": post.num_comments
        })
    return posts

def transform_data(posts):
    df = pd.DataFrame(posts)
    print("Datos transformados:")
    print(df.head())
    return df

def load_data_to_db(df, db_name):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS reddit_posts (
            id TEXT PRIMARY KEY,
            title TEXT,
            score INTEGER,
            author TEXT,
            created_utc DATETIME,
            num_comments INTEGER
        )
    ''')

    # Insertar datos
    df.to_sql('reddit_posts', conn, if_exists='append', index=False)

    conn.commit()
    conn.close()
    print("Datos cargados en la base de datos.")

if __name__ == "__main__":
    CLIENT_ID = "yGIuSP1W0oX7oMULcg6uXw" 
    CLIENT_SECRET = "DvnDuXDNcn3hgGprOdP-Ks-b2l5nFg"  
    USER_AGENT = "script:data-engineering:v1.0 (by /u/insertusernamehere)"

    SUBREDDIT_NAME = "dataengineering"  # Cambia al subreddit que desees
    DB_NAME = "C:\\Users\\Usuario\\Desktop\\sqlite-tools-win-x64-3470200\\reddit_data.db"

    try:
        print("Configurando API de Reddit...")
        reddit = configure_reddit(CLIENT_ID, CLIENT_SECRET, USER_AGENT)

        print("Extrayendo datos del subreddit...")
        raw_posts = extract_posts(reddit, SUBREDDIT_NAME, limit=20)

        print("Transformando datos...")
        transformed_data = transform_data(raw_posts)

        print("Cargando datos en la base de datos...")
        load_data_to_db(transformed_data, DB_NAME)

        print("Pipeline completado")
    except Exception as e:
        print(f"Error en el pipeline: {e}")
