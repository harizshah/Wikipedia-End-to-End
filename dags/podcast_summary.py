from airflow.decorators import dag, task

import pendulum
import requests

@dag(
    dag_id='podcast_summary',
    schedule_interval="@daily",
    start_date=pendulum.datetime(2023, 11, 12),
    catchup=False,
)

def podcast_summary():

    @task()
    def get_episodes():
        import xmltodict
        data=requests.get("https://www.marketplace.org/feed/podcast/marketplace/")
        feed= xmltodict.parse(data.text)
        episodes = feed['rss']['channel']['item']
        print(f"Found {len(episodes)} episodes.")
        return episodes
    
    podcast_episodes = get_episodes()

summary = podcast_summary()    
