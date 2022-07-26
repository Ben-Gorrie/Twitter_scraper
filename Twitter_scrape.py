# Databricks notebook source
# MAGIC %sh
# MAGIC curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
# MAGIC curl https://packages.microsoft.com/config/ubuntu/16.04/prod.list > /etc/apt/sources.list.d/mssql-release.list
# MAGIC sudo apt-get update
# MAGIC sudo ACCEPT_EULA=Y apt-get -q -y install msodbcsql17

# COMMAND ----------

#import relevant libraries
import pandas as pd
from pandas import json_normalize
import tweepy
import re
from datetime import datetime
import pyodbc 
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

# COMMAND ----------

## Connects to Database
def connect_to_db():
    username = 'GENERIC_SQL_USER'
    password = dbutils.secrets.get(scope="azurekv-scope",key='sqlPwd')
    server_name = 'ekiodp-dev-sql-01'
    db_name = 'ekiodp-dev-sql-01-db-01'
    driver = '{ODBC Driver 17 for SQL Server}'

    #méthode pyodbc
    cnxn = pyodbc.connect(f"""Driver={driver};
                          Server=tcp:{server_name}.database.windows.net,1433;
                          Database={db_name};
                          Uid={username};Pwd={password}; ;
                          Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30; """)
    return cnxn

# COMMAND ----------

connection = connect_to_db()
cursor = connection.cursor()

engine = create_engine("mssql+pyodbc://", poolclass=StaticPool, creator=lambda: connection)


#trigger_date_time = datetime.now().replace(microsecond=0)
#source_id = 2

trigger_date_time = datetime.strptime(dbutils.widgets.get('trigger_date_time').split(".")[0],"%Y-%m-%dT%H:%M:%S")
source_id = dbutils.widgets.get('source_id')

parameters_df =  pd.read_sql(f"SELECT * FROM dbo.SourcesParameters WHERE source_id ={source_id}",connection)

#Parameters for main function
count = parameters_df.loc[parameters_df["parameter_name"] == "count", "parameter_value"].to_string(index = False)
language = parameters_df.loc[parameters_df["parameter_name"] == "language", "parameter_value"].to_string(index = False)
query = parameters_df.loc[parameters_df["parameter_name"] == "query", "parameter_value"].to_string(index = False)
sort_by = parameters_df.loc[parameters_df["parameter_name"] == "sort_by", "parameter_value"].to_string(index = False)

#Parameters for api access
API_key = dbutils.secrets.get(scope="azurekv-scope",key="API-key")
API_secret_key = dbutils.secrets.get(scope="azurekv-scope",key="API-secret-key")
Access_token = dbutils.secrets.get(scope="azurekv-scope",key="Access-token")
Access_token_secret = dbutils.secrets.get(scope="azurekv-scope",key="Access-token-secret")

# COMMAND ----------

#Authenticate using twitter key and secret key 
auth = tweepy.OAuth1UserHandler(API_key, API_secret_key, Access_token, Access_token_secret)
api = tweepy.API(auth)

try:
    api.verify_credentials()
    print("Auth OK")
except:
    print("Auth error")

# COMMAND ----------

def get_data_from_twitter(query: str, language: str, sort_by : str, ct : int) -> pd.DataFrame:
    """
    Search twitter for tweets containing certain keywords and return resulting tweets in a dataframe
    
    :param query: String of words to search
    :param language: Language of tweets that get searched
    :param sort_by: Can either be popular, mixed or recent. 
    :param ct: Number of tweets to search in one go, up to a maximum of 100
    :return: pd.Dataframe of tweets
    """
    start_date_time = datetime.now().replace(microsecond=0)
    tweets = api.search_tweets(q = query, lang = language, result_type = sort_by, count = ct, tweet_mode = "extended")

    json_tweet_list = []
    for tweet in tweets:
        if "retweeted_status" in "tweet":
            print("Retweet")
            continue
        media_links = []
        if "media" in tweet.entities:
            media = tweet.entities["media"]
            for url in media:
                media_links.append(url["expanded_url"])
        media_links = " ".join(media_links)
        json_tweet_list.append({
                "source_id" : source_id,
                "trigger_date_time" : trigger_date_time,
                "execution_date_time" : start_date_time,
                "published_time": tweet.created_at.replace(tzinfo=None),
                "tweet_id" : tweet.id_str,
                "tweet_reply_id" : tweet.in_reply_to_status_id_str,
                "keywords" : query,
                "tweet": re.sub(r"http\S+", "", tweet.full_text), 
                "nb_of_retweets" : tweet.retweet_count,
                "nb_of_likes" : tweet.favorite_count,
                "media_links" : media_links,
                "language" : language
                #need premium/enterprise tier for this "reply_count" : tweet.reply_count
            })

    df = pd.json_normalize(json_tweet_list)
    return df

# COMMAND ----------

def scrapper_main_twitter(query: str, language: str, sort_by : str, ct : int) -> None:
    """
    Get data from twitter and add it to ODE database
    :param query: String of words to search
    :param language: Language of tweets that get searched
    :param sort_by: Can either be popular, mixed or recent. 
    :param ct: Number of tweets to search in one go, up to a maximum of 100
    :return: pd.Dataframe of tweets
    """
    start_date_time = datetime.now().replace(microsecond=0)
    
    cursor.execute(f"INSERT INTO dbo.ExtractionResults (source_id, trigger_date_time, start_date_time, end_date_time, status, nb_rows, error_message)values(?,?,?,?,?,?,?)",source_id, trigger_date_time, start_date_time, datetime.fromtimestamp(0), "WIP", 0, None)
    connection.commit()
    
    try:
    
        df = get_data_from_twitter(query = query, language = language, sort_by = sort_by, ct = ct)
        df["source_id"] = source_id
        df["trigger_date_time"] = trigger_date_time
        df["execution_date_time"] = start_date_time
        print("Got data")
        
        """for index, row in df.iterrows():
            cursor.execute(f"INSERT INTO Twitter.Tweets (source_id, trigger_date_time, execution_date_time, published_time, tweet_id, tweet_reply_id, keywords, tweet, nb_of_retweets, nb_of_likes, media_links, language) values(?,?,?,?,?,?,?,?,?,?,?,?)", row.source_id, row.trigger_date_time, row.execution_date_time, row.published_time, row.tweet_id, row.tweet_reply_id, row.keywords, row.tweet, row.nb_of_retweets, row.nb_of_likes, row.media_links, row.language)
            connection.commit()"""
        df.to_sql('Tweets', con=engine, schema = "Twitter", if_exists='append',method=None, index = False)
        cursor.execute(f"UPDATE dbo.ExtractionResults SET status=?, end_date_time=?,nb_rows=?,error_message=? WHERE start_date_time=?","OK",datetime.now(),df.shape[0],None,start_date_time)
        connection.commit()
        cursor.close()
        
    except Exception as ex:
        print(f"Error: {ex}")
        cursor.execute(f"UPDATE dbo.ExtractionResults SET status=?, end_date_time=?,nb_rows=?,error_message=? WHERE start_date_time=?","FAILED",datetime.now(),0,f"{ex}"[0:1990],start_date_time)
        connection.commit()
        cursor.close()
        
        

# COMMAND ----------

scrapper_main_twitter(query, language, sort_by, count)
