import pandas as pd
import mysql.connector
import re
from sqlalchemy import create_engine
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Function to connect to MySQL database
def create_db_connection():
    try:
        mydb = mysql.connector.connect(
            host="localhost",
            user="root",
            password="yourpassword",
            database="youtube_data"
        )
        return mydb
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return None

# Drop database used here if any error happened in data types of the table created 
mydb = create_db_connection()
if mydb is not None:
    mycursor = mydb.cursor(buffered=True)
    mycursor.execute('DROP DATABASE IF EXISTS youtube_data')

# Used Google API key to fetch the data from YouTube 
def Api_connector():
    apikey = "yourAPI-key"
    api_service_name = "youtube"
    api_version = "v3"
    youtube = build(api_service_name, api_version, developerKey=apikey)
    return youtube

youtube = Api_connector()

# Function to fetch channel details
def channel_info(channel_id):
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id
    )
    response = request.execute()
    data = {
        "Channel_Name": response["items"][0]["snippet"]["title"],
        "Channel_Id": response["items"][0]["id"],
        "Channel_Des": response["items"][0]["snippet"]["description"],
        "Channel_playid": response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"],
        "channel_viewcount": response["items"][0]["statistics"]["viewCount"],
        "channel_subcount": response["items"][0]["statistics"]["subscriberCount"]
    }
    df = pd.DataFrame(data, index=[0])
    return df

# Fetch data and insert into MySQL database
def eachchanneldetails(channel_ids):
    mydb = create_db_connection()
    if mydb is None:
        return  # Exit if the connection fails

    mycursor = mydb.cursor(buffered=True)
    connection_str = f"mysql+mysqlconnector://root:yourpassword@localhost/youtube_data"
    engine = create_engine(connection_str)

    mycursor.execute('CREATE DATABASE IF NOT EXISTS youtube_data')
    mycursor.execute('USE youtube_data')
    mycursor.execute('''
        CREATE TABLE IF NOT EXISTS channels (
            channel_name VARCHAR(100),
            channel_id VARCHAR(100),
            channel_des TEXT,
            channel_playid VARCHAR(50),
            channel_viewcount BIGINT,
            channel_subcount BIGINT
        )
    ''')
    mydb.commit()

    for channel_id in channel_ids:
        df = channel_info(channel_id)
        df.to_sql(name='channels', con=engine, if_exists='append', index=False)

    mycursor.close()
    mydb.close()

channel_ids = ['UCVlNQ5Olu3Uiv5FL8e-yEmQ']
eachchanneldetails(channel_ids)

# Function to fetch video ids from playlist id using Channel id
def playlist_videos_id(channel_ids):
    all_video_ids = []
    for channels_id in channel_ids:
        videos_ids = []
        response = youtube.channels().list(
            part="contentDetails",
            id=channels_id
        ).execute()
        playlist_Id = response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
    
        nextPageToken = None
    
        while True:
            response2 = youtube.playlistItems().list(
                part="snippet",
                playlistId=playlist_Id,
                maxResults=50,
                pageToken=nextPageToken
            ).execute()
            for i in range(len(response2["items"])):
                videos_ids.append(response2["items"][i]["snippet"]["resourceId"]["videoId"])
            nextPageToken = response2.get("nextPageToken")
            
            if nextPageToken is None:
                break
        all_video_ids.extend(videos_ids)        
    return all_video_ids

# Function for converting the hours to Seconds  
def iso8601_duration_to_seconds(duration):
    match = re.match(r'^PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?$', duration)
    if not match:
        return None

    hours = int(match.group(1)) if match.group(1) else 0
    minutes = int(match.group(2)) if match.group(2) else 0
    seconds = int(match.group(3)) if match.group(3) else 0

    total_seconds = (hours * 3600) + (minutes * 60) + seconds
    return total_seconds

# Function to fetch video data from video id    
def videos_data(all_video_ids):
    video_info = []
    for each in all_video_ids:
        request = youtube.videos().list(
            part='snippet,contentDetails,statistics',
            id=each
        )
        response = request.execute()
        for i in response["items"]:
            given = {
                "Video_Id": i["id"],
                "Video_Title": i["snippet"]["title"],
                "Video_Description": i["snippet"]["description"],
                "Channel_id": i['snippet']['channelId'],
                "video_Tags": i['snippet'].get("tags", 0),
                "video_Pubdate": i["snippet"]["publishedAt"],
                "Video_viewcount": i["statistics"]["viewCount"],
                "Video_likecount": i["statistics"].get('likeCount', 0),
                "Video_favoritecount": i["statistics"]["favoriteCount"],
                "Video_commentcount": i["statistics"].get("commentCount", 0),
                "Video_duration": iso8601_duration_to_seconds(i["contentDetails"]["duration"]),
                "Video_thumbnails": i["snippet"]["thumbnails"]['default']['url'],
                "Video_caption": i["contentDetails"]["caption"]
            }
                   
            video_info.append(given)
    df1 = pd.DataFrame(video_info)    
    return df1

allvideo_ids = playlist_videos_id(channel_ids)
df1 = videos_data(allvideo_ids)

# Insert video data into MySQL database
mydb = create_db_connection()
if mydb is None:
    raise Exception("Failed to connect to the database.")

mycursor = mydb.cursor(buffered=True)
connection_str = f"mysql+mysqlconnector://root:yourpassword@localhost/youtube_data"
engine = create_engine(connection_str)

mycursor.execute('USE youtube_data')
mycursor.execute('''
    CREATE TABLE IF NOT EXISTS videos (
        Video_Id VARCHAR(50),
        Video_Title VARCHAR(200),
        Video_Description TEXT,
        Channel_id VARCHAR(50),
        video_Tags TEXT,
        Video_pubdate VARCHAR(200),
        Video_viewcount BIGINT,
        Video_likecount BIGINT,
        Video_favoritecount INT(15),
        Video_commentcount BIGINT,
        Video_duration INT,
        Video_thumbnails TEXT,
        Video_caption VARCHAR(10)
    )
''')
mydb.commit()

df1.to_sql(name='videos', con=engine, if_exists='append', index=False)

mycursor.close()
mydb.close()

# Function to fetch comments from all videos
def comments_inf(allvideo_ids):
    commentdata = []
    try:
        for video in allvideo_ids:
            nextpagetoken = None
            while True:
                try:
                    request = youtube.commentThreads().list(
                        part="snippet",
                        videoId=video,
                        maxResults=50,
                        pageToken=nextpagetoken
                    )
                    response = request.execute()
                    
                    for k, all in enumerate(response["items"]):
                        given = {
                            "Comment_Id": all["snippet"]["topLevelComment"]["id"],
                            "Comment_Text": all["snippet"]["topLevelComment"]["snippet"]["textDisplay"],
                            "Comment_Authorname": all["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                            "published_date": all["snippet"]["topLevelComment"]["snippet"]["publishedAt"],
                            "video_id": all["snippet"]["topLevelComment"]["snippet"]["videoId"],
                            'channel_id': all['snippet']['channelId']
                        }
                                
                        commentdata.append(given)
                    nextpagetoken = response.get('nextPageToken')
                    if nextpagetoken is None:
                        break
                except HttpError as e:
                    if e.resp.status == 403:
                        print(f"Comments are disabled for video ID: {video}")
                        break
                    else:
                        raise
    except Exception as e:
        print(f"An error occurred: {e}")
    df2 = pd.DataFrame(commentdata)
    return df2

df2 = comments_inf(allvideo_ids)

# Insert comments data into MySQL database
mydb = create_db_connection()
if mydb is None:
    raise Exception("Failed to connect to the database.")

mycursor = mydb.cursor(buffered=True)
connection_str = f"mysql+mysqlconnector://root:yourpassword@localhost/youtube_data"
engine = create_engine(connection_str)

mycursor.execute('USE youtube_data')
mycursor.execute('''
    CREATE TABLE IF NOT EXISTS comments (
        comment_id VARCHAR(30),
        Comment_Text TEXT,
        comment_authorname VARCHAR(255),
        published_date VARCHAR(200),
        video_id VARCHAR(40),
        channel_id VARCHAR(50)
    )
''')

mydb.commit()
df2.to_sql(name='comments', con=engine, if_exists='append', index=False)

mycursor.close()
mydb.close()
import pandas as pd
import mysql.connector
import re
from sqlalchemy import create_engine
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Function to connect to MySQL database
def create_db_connection():
    try:
        mydb = mysql.connector.connect(
            host="localhost",
            user="root",
            password="yourpassword",
            database="youtube_data"
        )
        return mydb
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return None

# Drop database used here if any error happened in data types of the table created 
mydb = create_db_connection()
if mydb is not None:
    mycursor = mydb.cursor(buffered=True)
    mycursor.execute('DROP DATABASE IF EXISTS youtube_data')

# Used Google API key to fetch the data from YouTube 
def Api_connector():
    apikey = "yourAPI-key"
    api_service_name = "youtube"
    api_version = "v3"
    youtube = build(api_service_name, api_version, developerKey=apikey)
    return youtube

youtube = Api_connector()

# Function to fetch channel details
def channel_info(channel_id):
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id
    )
    response = request.execute()
    data = {
        "Channel_Name": response["items"][0]["snippet"]["title"],
        "Channel_Id": response["items"][0]["id"],
        "Channel_Des": response["items"][0]["snippet"]["description"],
        "Channel_playid": response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"],
        "channel_viewcount": response["items"][0]["statistics"]["viewCount"],
        "channel_subcount": response["items"][0]["statistics"]["subscriberCount"]
    }
    df = pd.DataFrame(data, index=[0])
    return df

# Fetch data and insert into MySQL database
def eachchanneldetails(channel_ids):
    mydb = create_db_connection()
    if mydb is None:
        return  # Exit if the connection fails

    mycursor = mydb.cursor(buffered=True)
    connection_str = f"mysql+mysqlconnector://root:yourpassword@localhost/youtube_data"
    engine = create_engine(connection_str)

    mycursor.execute('CREATE DATABASE IF NOT EXISTS youtube_data')
    mycursor.execute('USE youtube_data')
    mycursor.execute('''
        CREATE TABLE IF NOT EXISTS channels (
            channel_name VARCHAR(100),
            channel_id VARCHAR(100),
            channel_des TEXT,
            channel_playid VARCHAR(50),
            channel_viewcount BIGINT,
            channel_subcount BIGINT
        )
    ''')
    mydb.commit()

    for channel_id in channel_ids:
        df = channel_info(channel_id)
        df.to_sql(name='channels', con=engine, if_exists='append', index=False)

    mycursor.close()
    mydb.close()

channel_ids = ['UCVlNQ5Olu3Uiv5FL8e-yEmQ']
eachchanneldetails(channel_ids)

# Function to fetch video ids from playlist id using Channel id
def playlist_videos_id(channel_ids):
    all_video_ids = []
    for channels_id in channel_ids:
        videos_ids = []
        response = youtube.channels().list(
            part="contentDetails",
            id=channels_id
        ).execute()
        playlist_Id = response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
    
        nextPageToken = None
    
        while True:
            response2 = youtube.playlistItems().list(
                part="snippet",
                playlistId=playlist_Id,
                maxResults=50,
                pageToken=nextPageToken
            ).execute()
            for i in range(len(response2["items"])):
                videos_ids.append(response2["items"][i]["snippet"]["resourceId"]["videoId"])
            nextPageToken = response2.get("nextPageToken")
            
            if nextPageToken is None:
                break
        all_video_ids.extend(videos_ids)        
    return all_video_ids

# Function for converting the hours to Seconds  
def iso8601_duration_to_seconds(duration):
    match = re.match(r'^PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?$', duration)
    if not match:
        return None

    hours = int(match.group(1)) if match.group(1) else 0
    minutes = int(match.group(2)) if match.group(2) else 0
    seconds = int(match.group(3)) if match.group(3) else 0

    total_seconds = (hours * 3600) + (minutes * 60) + seconds
    return total_seconds

# Function to fetch video data from video id    
def videos_data(all_video_ids):
    video_info = []
    for each in all_video_ids:
        request = youtube.videos().list(
            part='snippet,contentDetails,statistics',
            id=each
        )
        response = request.execute()
        for i in response["items"]:
            given = {
                "Video_Id": i["id"],
                "Video_Title": i["snippet"]["title"],
                "Video_Description": i["snippet"]["description"],
                "Channel_id": i['snippet']['channelId'],
                "video_Tags": i['snippet'].get("tags", 0),
                "video_Pubdate": i["snippet"]["publishedAt"],
                "Video_viewcount": i["statistics"]["viewCount"],
                "Video_likecount": i["statistics"].get('likeCount', 0),
                "Video_favoritecount": i["statistics"]["favoriteCount"],
                "Video_commentcount": i["statistics"].get("commentCount", 0),
                "Video_duration": iso8601_duration_to_seconds(i["contentDetails"]["duration"]),
                "Video_thumbnails": i["snippet"]["thumbnails"]['default']['url'],
                "Video_caption": i["contentDetails"]["caption"]
            }
                   
            video_info.append(given)
    df1 = pd.DataFrame(video_info)    
    return df1

allvideo_ids = playlist_videos_id(channel_ids)
df1 = videos_data(allvideo_ids)

# Insert video data into MySQL database
mydb = create_db_connection()
if mydb is None:
    raise Exception("Failed to connect to the database.")

mycursor = mydb.cursor(buffered=True)
connection_str = f"mysql+mysqlconnector://root:yourpassword@localhost/youtube_data"
engine = create_engine(connection_str)

mycursor.execute('USE youtube_data')
mycursor.execute('''
    CREATE TABLE IF NOT EXISTS videos (
        Video_Id VARCHAR(50),
        Video_Title VARCHAR(200),
        Video_Description TEXT,
        Channel_id VARCHAR(50),
        video_Tags TEXT,
        Video_pubdate VARCHAR(200),
        Video_viewcount BIGINT,
        Video_likecount BIGINT,
        Video_favoritecount INT(15),
        Video_commentcount BIGINT,
        Video_duration INT,
        Video_thumbnails TEXT,
        Video_caption VARCHAR(10)
    )
''')
mydb.commit()

df1.to_sql(name='videos', con=engine, if_exists='append', index=False)

mycursor.close()
mydb.close()

# Function to fetch comments from all videos
def comments_inf(allvideo_ids):
    commentdata = []
    try:
        for video in allvideo_ids:
            nextpagetoken = None
            while True:
                try:
                    request = youtube.commentThreads().list(
                        part="snippet",
                        videoId=video,
                        maxResults=50,
                        pageToken=nextpagetoken
                    )
                    response = request.execute()
                    
                    for k, all in enumerate(response["items"]):
                        given = {
                            "Comment_Id": all["snippet"]["topLevelComment"]["id"],
                            "Comment_Text": all["snippet"]["topLevelComment"]["snippet"]["textDisplay"],
                            "Comment_Authorname": all["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                            "published_date": all["snippet"]["topLevelComment"]["snippet"]["publishedAt"],
                            "video_id": all["snippet"]["topLevelComment"]["snippet"]["videoId"],
                            'channel_id': all['snippet']['channelId']
                        }
                                
                        commentdata.append(given)
                    nextpagetoken = response.get('nextPageToken')
                    if nextpagetoken is None:
                        break
                except HttpError as e:
                    if e.resp.status == 403:
                        print(f"Comments are disabled for video ID: {video}")
                        break
                    else:
                        raise
    except Exception as e:
        print(f"An error occurred: {e}")
    df2 = pd.DataFrame(commentdata)
    return df2

df2 = comments_inf(allvideo_ids)

# Insert comments data into MySQL database
mydb = create_db_connection()
if mydb is None:
    raise Exception("Failed to connect to the database.")

mycursor = mydb.cursor(buffered=True)
connection_str = f"mysql+mysqlconnector://root:yourpassword@localhost/youtube_data"
engine = create_engine(connection_str)

mycursor.execute('USE youtube_data')
mycursor.execute('''
    CREATE TABLE IF NOT EXISTS comments (
        comment_id VARCHAR(30),
        Comment_Text TEXT,
        comment_authorname VARCHAR(255),
        published_date VARCHAR(200),
        video_id VARCHAR(40),
        channel_id VARCHAR(50)
    )
''')

mydb.commit()
df2.to_sql(name='comments', con=engine, if_exists='append', index=False)

mycursor.close()
mydb.close()
