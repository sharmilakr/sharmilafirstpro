import pandas as pd
from googleapiclient.discovery import build
import requests
import json
import streamlit as st
from streamlit_option_menu import option_menu
from datetime import datetime
from PIL import Image
import matplotlib.pyplot as plt
from pymongo import MongoClient
import pymysql
import mysql.connector as sql
import plotly.express as px

#setting page config
st.set_page_config(page_title='Youtube Data Harvesting & Warehousing',layout="wide",initial_sidebar_state="expanded")
st.header(':green[Youtube Data Harvesting & Warehousing]',divider = 'rainbow')
#setting options
with st.sidebar:
    selected = option_menu(None, ["Home","Collect and Transform","View"], 
                           icons=["house-door-fill","tools","card-text"],
                           default_index=0,
                           orientation="vertical",
                           styles={"nav-link": {"font-size": "30px", "text-align": "centre", "margin": "0px", 
                                                "--hover-color": "#C80101"},
                                   "icon": {"font-size": "30px"},
                                   "container" : {"max-width": "5000px"},
                                   "nav-link-selected": {"background-color": "#C80101"}}
                            )
# home_page
if selected == "Home":
        col1,col2 = st.columns(2,gap= 'medium')
        col1.markdown("## :blue[Domain] : Social Media")
        col1.markdown("## :blue[Technologies used] : Python,MongoDB, Youtube Data API, MySql, Streamlit")
        col1.markdown("## :blue[Overview] : Retrieving the Youtube channels data from the Google API, storing it in a MongoDB as data lake, migrating and transforming data into a SQL database,then querying the data and displaying it in the Streamlit app.")
        col2.markdown("#   ")
        col2.markdown("#   ")
        col2.markdown("#   ")

#building connection with youtube api
api_key="*************************"
youtube = build('youtube', 'v3', developerKey=api_key)


#function to get channel details
def get_channel_details(channel_id):
    cha_data = []
    request = youtube.channels().list(part='snippet,contentDetails,statistics', id=channel_id)
    response = request.execute()
    for i in range(len(response['items'])):
        data = dict(Channel_id=channel_id[i],
                Channel_name=response['items'][i]['snippet']['title'],
                Playlist_id = response['items'][i]['contentDetails']['relatedPlaylists']['uploads'],
                Total_Subscribers=response['items'][i]['statistics']['subscriberCount'],
                Total_videos=response['items'][i]['statistics']['videoCount'],
                Channel_description=response['items'][i]['snippet']['description'],
                Channel_view_count = response['items'][i]['statistics']['viewCount'],
                Country = response['items'][i]['snippet'].get('country'))
        cha_data.append(data)
    return cha_data

# function to get video_ids
def get_channel_videos(channel_id):
    video_ids = []
    res = youtube.channels().list(id=channel_id,part='contentDetails').execute()
    playlist_id = res['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None
    while True:
        res = youtube.playlistItems().list(playlistId=playlist_id, 
                                           part='snippet', 
                                           maxResults=50,
                                           pageToken=next_page_token).execute()
        for i in range(len(res['items'])):
            video_ids.append(res['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = res.get('nextPageToken')
        if next_page_token is None:
            break
        return video_ids
    
    #function to get video details
def get_video_details(video_ids):
    video_stats = []
    for i in range(0, len(video_ids), 50):
        response = youtube.videos().list(
                    part="snippet,contentDetails,statistics",
                    id=','.join(video_ids[i:i+50])).execute()
        for video in response['items']:
            video_details = dict(Channel_name = video['snippet']['channelTitle'],
                                Channel_id = video['snippet']['channelId'],
                                Video_id = video['id'],
                                Title = video['snippet']['title'],
                                Tags = video['snippet'].get('tags'),
                                Thumbnail = video['snippet']['thumbnails']['default']['url'],
                                Channel_description = video['snippet']['description'],
                                Published_date = video['snippet']['publishedAt'],
                                Duration = video['contentDetails']['duration'],
                                Views = video['statistics']['viewCount'],
                                Likes = video['statistics'].get('likeCount'),
                                Comments = video['statistics'].get('commentCount'),
                                Favorite_count = video['statistics']['favoriteCount'],
                                Definition = video['contentDetails']['definition'],
                                Caption_status = video['contentDetails']['caption']
                               )
            video_stats.append(video_details)
        return video_stats
    
    #fuction to get comment details
def get_comments_details(v_id):
    comment_data = []
    try:
        next_page_token = None
        while True:
            response = youtube.commentThreads().list(part="snippet,replies",
                                                    videoId=v_id,
                                                    maxResults=100,
                                                    pageToken=next_page_token).execute()
            for cmt in response['items']:
                data = dict(Comment_id = cmt['id'],
                            Video_id = cmt['snippet']['videoId'],
                            Comment_text = cmt['snippet']['topLevelComment']['snippet']['textDisplay'],
                            Comment_posted_date=cmt['snippet']['topLevelComment']['snippet']['publishedAt'],
                            Comment_author = cmt['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            Like_count = cmt['snippet']['topLevelComment']['snippet']['likeCount'],
                            Reply_count = cmt['snippet']['totalReplyCount']
                           )
                comment_data.append(data)

            next_page_token = response.get('nextPageToken')
            if next_page_token is None:
                break
    except:
        pass
    return comment_data

#mongodb and sql connection
local_client=MongoClient("mongodb://localhost:27017")
db=local_client["youtube_data"]

sql_db=sql.connect(host="127.0.0.1",user="root",password="******",database="youtube data harvesting")
mycursor=sql_db.cursor()


#mongodb
def channel_names():   
    ch_name = []
    for i in db.channel_details.find():
        ch_name.append(i['Channel_name'])
    return ch_name
#option menu

if selected == "Collect and Transform":
    tab1,tab2 = st.tabs(["$\huge COLLECT $", "$\huge TRANSFORM $"])

        
    with tab1:
        st.markdown("#    ")
        st.write("### Enter YouTube Channel_ID below :")
        ch_id = st.text_input("Hint : Goto channel's home page > Right click > View page source > Find channel_id").split(',')
            
        if ch_id and st.button("Extract Data"):
            ch_details = get_channel_details(ch_id)
            st.write(f'#### Extracted data from :green["{ch_details[0]["Channel_name"]}"] channel')
            st.table(ch_details)
                
        if st.button("Upload to MongoDB"):
            with st.spinner('Please Wait for it...'):
                ch_details = get_channel_details(ch_id)
                video_ids = get_channel_videos(ch_id)
                vid_details = get_video_details(video_ids)

                def comments():
                    com_d = []
                    for i in video_ids:                         
                        com_d += get_comments_details(i)
                    return com_d
                comm_details = comments()

                collections1 = db.channel_details
                collections1.insert_many(ch_details)

                collections2 = db.video_details
                collections2.insert_many(vid_details)

                collections3 = db.comments_details
                collections3.insert_many(comm_details)
                st.success("Upload to MogoDB successful !!")
    #transform tab

    with tab2:
        st.markdown("#   ")
        st.markdown("### Select a channel to begin Transformation to SQL")

        ch_names = channel_names()
        user_inp = st.selectbox("Select channel", options=ch_names)
        
        def insert_into_channels(channel_name):
            collections = db.channel_details
            query = """INSERT INTO channel (Channel_id, Channel_name, Playlist_id,Total_Subscribers , Channel_view_count, Total_videos, Channel_description, Country) 
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""
            for i in collections.find({"Channel_name": channel_name}):
                t = (
                    i['Channel_id'],
                    i['Channel_name'],
                    i['Playlist_id'],
                    i['Total_Subscribers'],
                    i['Channel_view_count'],
                    i['Total_videos'],
                    i['Channel_description'],
                    i['Country']
                )
                mycursor.execute(query, t)          
# Function to insert data into 'videos' table
        def insert_into_videos(channel_name):
            try:
                # Create a cursor by querying the MongoDB collection
                video_data_cursor = db.video_details.find({"Channel_name": channel_name})  # Filter by selected channel

                for video_data in video_data_cursor:
                    # Convert the 'Published_date' string to a MySQL-compatible format
                    published_date = datetime.strptime(video_data["Published_date"], "%Y-%m-%dT%H:%M:%SZ").strftime(
                        "%Y-%m-%d %H:%M:%S")

                    # Convert the 'Tags' list to a JSON string
                    tags_json = json.dumps(video_data["Tags"])

                    # Convert the 'Caption_status' string to a boolean
                    caption_status = int(video_data["Caption_status"] == "true")

                    # Insert data into the 'videos' table
                    query = """INSERT INTO videos (
                        id,
                        Video_id,
                        Channel_name,
                        Channel_id,
                        Title,
                        Tags,
                        Thumbnail,
                        Channel_description,
                        Published_date,
                        Duration,
                        Views,
                        Likes,
                        Comments,
                        Favorite_count,
                        Definition,
                        Caption_status
                    ) VALUES (NULL, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

                    mycursor.execute(query, (
                        video_data["Video_id"],
                        video_data["Channel_name"],
                        video_data["Channel_id"],
                        video_data["Title"],
                        tags_json,
                        video_data["Thumbnail"],
                        video_data["Channel_description"],
                        published_date,
                        video_data["Duration"],
                        video_data["Views"],
                        video_data["Likes"],
                        video_data["Comments"],
                        video_data["Favorite_count"],
                        video_data["Definition"],
                        caption_status
                    ))

                sql_db.commit()  # Commit the changes to MySQL after all videos are inserted
            except Exception as e:
                # Handle the exception, you can log it or take appropriate action
                st.error(f"Error during video data insertion: {str(e)}")


        # Function to insert data into 'comments' table
        def insert_into_comments(channel_name):
            collections1 = db.video_details
            collections2 = db.comments_details
            query = """INSERT INTO comments (Comment_id, Video_id, Comment_text, Comment_author, Comment_posted_date, Like_count, Reply_count) 
                       VALUES (%s, %s, %s, %s, %s, %s, %s)"""

            for vid in collections1.find({"Channel_name": channel_name}):
                for i in collections2.find({'Video_id': vid['Video_id']}):
                    try:
                        # Convert ISO 8601 datetime to MySQL datetime format
                        iso_datetime = i.get('Comment_posted_date')
                        if iso_datetime:
                            parsed_datetime = datetime.strptime(iso_datetime, '%Y-%m-%dT%H:%M:%SZ')
                            mysql_datetime = parsed_datetime.strftime('%Y-%m-%d %H:%M:%S')
                        else:
                            # Handle missing or invalid datetime gracefully
                            mysql_datetime = '0000-00-00 00:00:00' # or another default value
                    
                    except ValueError as e:
                        print("Error during transformation:", e)
                        # Handle invalid datetime format gracefully
                        mysql_datetime = '0000-00-00 00:00:00'  # or another default value
                                                       
                    t = (
                        i['Comment_id'],
                        i['Video_id'],
                        i['Comment_text'],
                        i['Comment_author'],
                        mysql_datetime,  # Use the converted datetime here
                        i['Like_count'],
                        i['Reply_count']
                    )
                    mycursor.execute(query, t)


        if st.button("Button"):
            try:
                st.text("Starting data transformation...")
                insert_into_channels(user_inp)
                insert_into_videos(user_inp)
                insert_into_comments(user_inp)
                sql_db.commit()
                st.success("Transformation to MySQL Successful!!!")
            except Exception as e:
                st.error(f"Error during transformation: {str(e)}")


        # VIEW PAGE
if selected == "View":

    st.write("## :orange[Select any question to get Insights]")
    questions = st.selectbox('Questions',
                             ['Click the question that you would like to query',
                              '1. What are the names of all the videos and their corresponding channels?',
                              '2. Which channels have the most number of videos, and how many videos do they have?',
                              '3. What are the top 10 most viewed videos and their respective channels?',
                              '4. How many comments were made on each video, and what are their corresponding video names?',
                              '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
                              '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
                              '7. What is the total number of views for each channel, and what are their corresponding channel names?',
                              '8. What are the names of all the channels that have published videos in the year 2022?',
                              '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
                              '10. Which videos have the highest number of comments, and what are their corresponding channel names?'])

    if questions == '1. What are the names of all the videos and their corresponding channels?':
        mycursor.execute(
            "SELECT title AS Video_Title, channel_name AS Channel_Name FROM videos ORDER BY channel_name")
        df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
        st.write(df)

    elif questions == '2. Which channels have the most number of videos, and how many videos do they have?':
        mycursor.execute("SELECT channel_name AS Channel_Name, total_videos AS Total_Videos FROM channel ORDER BY total_videos DESC")
        df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
        st.write(df)
        st.write("### :green[Number of videos in each channel :]")

        fig = px.bar(df,
                     x=mycursor.column_names[0],
                     y=mycursor.column_names[1],
                     orientation='v',
                     color=mycursor.column_names[0]
                     )
        st.plotly_chart(fig, use_container_width=True)

    elif questions == '3. What are the top 10 most viewed videos and their respective channels?':
        mycursor.execute("SELECT channel_name AS Channel_Name, title AS Video_Title, views AS Views FROM videos ORDER BY views DESC LIMIT 10")
        df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
        st.write(df)
        st.write("### :green[Top 10 most viewed videos :]")
        fig = px.bar(df,
                     x=mycursor.column_names[2],
                     y=mycursor.column_names[1],
                     orientation='h',
                     color=mycursor.column_names[0]
                     )
        st.plotly_chart(fig, use_container_width=True)

    elif questions == '4. How many comments were made on each video, and what are their corresponding video names?':
        mycursor.execute("SELECT a.video_id AS Video_id, a.title AS Video_Title, b.Total_Comments FROM videos AS a LEFT JOIN (SELECT video_id,COUNT(comment_id) AS Total_Comments FROM comments GROUP BY video_id) AS b ON a.video_id = b.video_id ORDER BY b.Total_Comments DESC")
        df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
        st.write(df)

    elif questions == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
        mycursor.execute("SELECT channel_name AS Channel_Name,title AS Title,likes AS Likes_Count FROM videos ORDER BY likes DESC LIMIT 10")
        df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
        st.write(df)
        st.write("### :green[Top 10 most liked videos :]")
        fig = px.bar(df,
                     x=mycursor.column_names[2],
                     y=mycursor.column_names[1],
                     orientation='h',
                     color=mycursor.column_names[0]
                     )
        st.plotly_chart(fig, use_container_width=True)

    elif questions == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
        mycursor.execute("""SELECT title AS Title, likes AS Likes_Count
                            FROM videos
                            ORDER BY likes DESC""")
        df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
        st.write(df)

    elif questions == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
        mycursor.execute("""SELECT channel_name AS Channel_Name, Channel_view_count AS Views
                            FROM channel
                            ORDER BY views DESC""")
        df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
        st.write(df)
        st.write("### :green[Channels vs Views :]")
        fig = px.bar(df,
                     x=mycursor.column_names[0],
                     y=mycursor.column_names[1],
                     orientation='v',
                     color=mycursor.column_names[0]
                     )
        st.plotly_chart(fig, use_container_width=True)

    elif questions == '8. What are the names of all the channels that have published videos in the year 2022?':
        mycursor.execute("""SELECT Channel_name AS Channel_Name
                            FROM videos
                            WHERE Published_date LIKE '2022%'
                            GROUP BY Channel_name
                            ORDER BY Channel_name""")
        df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
        st.write(df)

    elif questions == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
        mycursor.execute("""SELECT Channel_name,
                        SUM(duration_sec) / COUNT(*) AS average_duration
                        FROM (
                            SELECT channel_name,
                            CASE
                                WHEN Duration REGEXP '^PT[0-9]+H[0-9]+M[0-9]+S$' THEN
                                TIME_TO_SEC(CONCAT(
                                SUBSTRING_INDEX(SUBSTRING_INDEX(Duration, 'H', 1), 'T', -1), ':',
                            SUBSTRING_INDEX(SUBSTRING_INDEX(Duration, 'M', 1), 'H', -1), ':',
                            SUBSTRING_INDEX(SUBSTRING_INDEX(Duration, 'S', 1), 'M', -1)
                            ))
                                WHEN Duration REGEXP '^PT[0-9]+M[0-9]+S$' THEN
                                TIME_TO_SEC(CONCAT(
                                '0:', SUBSTRING_INDEX(SUBSTRING_INDEX(Duration, 'M', 1), 'T', -1), ':',
                                SUBSTRING_INDEX(SUBSTRING_INDEX(Duration, 'S', 1), 'M', -1)
                            ))
                                WHEN Duration REGEXP '^PT[0-9]+S$' THEN
                                TIME_TO_SEC(CONCAT('0:0:', SUBSTRING_INDEX(SUBSTRING_INDEX(Duration, 'S', 1), 'T', -1)))
                                END AS duration_sec
                        FROM videos
                        ) AS subquery
                        GROUP BY channel_name""")
        df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names
                          )
        st.write(df)
        st.write("### :green[Average video duration for channels :]")



    elif questions == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
        mycursor.execute("""SELECT Channel_name AS Channel_Name,Video_id AS Video_ID,Comments AS Comments
                            FROM videos
                            ORDER BY Comments DESC
                            LIMIT 10""")
        df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
        st.write(df)
        st.write("### :green[Videos with most comments :]")
        fig = px.bar(df,
                     x=mycursor.column_names[1],
                     y=mycursor.column_names[2],
                     orientation='v',
                     color=mycursor.column_names[0]
                     )
        st.plotly_chart(fig, use_container_width=True)

        




    


    
    
    
                    


        
            
            
            


        
        
    



    




    


 

                   
    

    

              

























