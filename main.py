from googleapiclient.discovery import build
import pymongo
import psycopg2
import pandas as pd
import streamlit as st

def API_info():
    """ Contains the API key and the service object"""

    api_key ='AIzaSyB0NqpRGh-gIMZsKdigSoqfa8CSU9VEEGM'
    #channel_id = 'UCnz-ZXXER4jOvuED5trXfEA'
    youtube = build('youtube','v3', developerKey= api_key)

    return youtube

youtube = API_info()

def get_channel_info(channel_id):
    """Gets the channel information"""

    request_ch = youtube.channels().list(
        part = 'snippet,contentDetails,statistics',
        id = channel_id
    )
    response_ch = request_ch.execute()

    for i in response_ch['items']:
        data_ch =dict(channel_name = i["snippet"]["title"],
                channel_id = i['id'],
                subscription_count = i['statistics']['subscriberCount'],
                channel_views = i['statistics']['viewCount'],
                channel_description = i['snippet']['description'],
                playlist_id = i['contentDetails']['relatedPlaylists']['uploads'])
    return data_ch

def get_video_id(channel_id):

    video_ID = []
    request = youtube.channels().list(
        part = 'contentDetails',
        id = channel_id
    )
    response = request.execute()
    playlistID =response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    
    nextpagetoken =None

    while True:
        request_vd = youtube.playlistItems().list(
            part="snippet",  
            maxResults=50,      
            playlistId=playlistID,
            pageToken =nextpagetoken        
        )
        response_vdid = request_vd.execute()
        
        
        for i in range(len(response_vdid['items'])):
            video_ID.append(response_vdid['items'][i]['snippet']['resourceId']['videoId'])
        nextpagetoken = response_vdid.get('nextPageToken')

        if nextpagetoken is None:
            break

    return video_ID

#video_IDt = get_video_id('UCnz-ZXXER4jOvuED5trXfEA')
#video_IDt

def get_video_info(channel_id,video_ID):

    video_info =[]   
    for video_id in video_ID:

        request = youtube.videos().list(
            part = "snippet,contentDetails,statistics",
            id = video_id
        )

        response_vd = request.execute()

        for i in response_vd['items']:
            data_vd = dict(ChannelName = i["snippet"]["channelTitle"],
                           ChannelId = i["snippet"]["channelId"],
                           VideoId = i["id"],
                           VideoName= i["snippet"]["title"],
                           VideoDescription = i['snippet'].get("description"),
                           tags = i['snippet'].get('tags'),
                           publishedAt = i["snippet"]["publishedAt"],
                           ViewCount = i['statistics'].get("viewCount"),
                           likeCount = i['statistics'].get("likeCount"),
                           commentCount = i['statistics'].get("commentCount"),
                           thumbnail = i["snippet"]["thumbnails"]["default"]["url"],
                           Duration = i["contentDetails"]["duration"],
                           CaptionStatus = i["contentDetails"]["caption"],
                           favoriteCount = i["statistics"]["favoriteCount"]
                           )
            video_info.append(data_vd)           
        
    return video_info

        
def comments_info(video_ID):

    cmt_data = []
    try:        
        for video_id in video_ID:
            request_cmt = youtube.commentThreads().list(
                part = "snippet",
                videoId = video_id,
                maxResults = 30
            )
            response_cmt = request_cmt.execute()

            for i in response_cmt["items"]:
                data_cmt = dict(CommentId = i["snippet"]["topLevelComment"]["id"],
                                videoID = i["snippet"]["videoId"],
                                CommentText = i["snippet"]["topLevelComment"]["snippet"]["textDisplay"],
                                CommentAuthor = i["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                                CommentPublishedAt = i["snippet"]["topLevelComment"]["snippet"]["publishedAt"]
                                )
                cmt_data.append(data_cmt)
            
        
    except:
        pass
    return cmt_data

# Connecting to MongoDB

#client =pymongo.MongoClient("mongodb+srv://akshayanandhiniks:Manu2212@cluster0.rtawerz.mongodb.net/?retryWrites=true&w=majority")
client = pymongo.MongoClient('localhost', 27017)
db = client["YoutubeData"]

def channel_details(channel_id):
    channel_info = get_channel_info(channel_id)
    VideoIDS = get_video_id(channel_id)
    Video_details = get_video_info(channel_id,VideoIDS)
    Comments = comments_info(VideoIDS)

    collection1 = db["ChannelDetails"]
    collection1.insert_one({"channelInformation": channel_info,"VideoDetails": Video_details,
                            "CommentDetails":Comments})
    
    return "upload complete"



def table_channel():
    mydatabase = psycopg2.connect(host = "localhost",
                            user = "postgres",
                            password = 'manumanu',
                            database = "YoutubeData",
                            port = "5432")

    cursor_db  = mydatabase.cursor()

    query_drop = '''drop table if exists channel'''
    cursor_db.execute(query_drop)
    mydatabase.commit()
    try:
        myquery = '''create table if not exists channel(channel_name varchar(255),
                                                  channel_id varchar(255) primary key,
                                                  subscription_count bigint,
                                                  channel_views bigint,
                                                  channel_description text,
                                                  playlist_id varchar(255) )  '''
                                                            
        cursor_db.execute(myquery)
        mydatabase.commit()
    except:
        print('cannot create query')

    db = client['YoutubeData']
    collection2 =db["ChannelDetails"]
    list_ch = []

    for ch_dt in collection2.find({},{"_id":0,"channelInformation":1}):
        list_ch.append(ch_dt["channelInformation"])
    df = pd.DataFrame(list_ch)

    for index,row in df.iterrows():
        query_insert = '''insert into channel(channel_name,
                                            channel_id,
                                            subscription_count,
                                            channel_views,
                                            channel_description,
                                            playlist_id )
                                            
                                            values(%s,%s,%s,%s,%s,%s)'''
        values = (row['channel_name'],
                row['channel_id'],
                row['subscription_count'],
                row['channel_views'],
                row['channel_description'],
                row['playlist_id'])
        
        cursor_db.execute(query_insert,values)  
        mydatabase.commit()

def table_video():
    mydatabase = psycopg2.connect(host = "localhost",
                            user = "postgres",
                            password = 'manumanu',
                            database = "YoutubeData",
                            port = "5432")

    cursor_db  = mydatabase.cursor()

    query_drop = '''drop table if exists video'''
    cursor_db.execute(query_drop)
    mydatabase.commit()
    try:
        myquery = '''create table if not exists video(channel_name varchar(255),
                                                    ChannelId varchar(255),
                                                    VideoId varchar(50) primary key,
                                                    VideoName varchar(255),
                                                    VideoDescription text,
                                                    tags text,
                                                    publishedAt timestamp,
                                                    ViewCount bigint,
                                                    likeCount bigint,
                                                    commentCount int,
                                                    thumbnail varchar(255),
                                                    Duration interval,
                                                    CaptionStatus varchar(255),
                                                    favoriteCount int )  '''
                                                            
        cursor_db.execute(myquery)
        mydatabase.commit()
    except:
        print('cannot create query')
    
    db = client['YoutubeData']
    collection2 =db["ChannelDetails"]
    list_vd = []

    for vd_dt in collection2.find({},{"_id":0,"VideoDetails":1}):
        for i in range(len(vd_dt["VideoDetails"])):
            list_vd.append(vd_dt["VideoDetails"][i])
    df1 = pd.DataFrame(list_vd)

    for index,row in df1.iterrows():
        query_insert = '''insert into video(channel_name,
                                            ChannelId,
                                            VideoId,
                                            VideoName,
                                            VideoDescription,
                                            tags,
                                            publishedAt,
                                            ViewCount,
                                            likeCount,
                                            commentCount,
                                            thumbnail,
                                            Duration,
                                            CaptionStatus,
                                            favoriteCount)
                                            
                                            values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        values = (row['ChannelName'],
                row['ChannelId'],
                row['VideoId'],
                row['VideoName'],
                row['VideoDescription'],
                row['tags'],
                row['publishedAt'],
                row['ViewCount'],
                row['likeCount'],
                row['commentCount'],
                row['thumbnail'],
                row['Duration'],
                row['CaptionStatus'],
                row['favoriteCount'])
        
        cursor_db.execute(query_insert,values)  
        mydatabase.commit()

def table_comment():
    mydatabase = psycopg2.connect(host = "localhost",
                            user = "postgres",
                            password = 'manumanu',
                            database = "YoutubeData",
                            port = "5432")

    cursor_db  = mydatabase.cursor()

    query_drop = '''drop table if exists comments'''
    cursor_db.execute(query_drop)
    mydatabase.commit()

    try:
        myquery = '''create table if not exists comments(CommentId varchar(255) primary key,
                                                        videoID varchar(255) ,
                                                        CommentText text,
                                                        CommentAuthor varchar(255),
                                                        CommentPublishedAt timestamp
                                                        )  '''
                                                            
        cursor_db.execute(myquery)
        mydatabase.commit()
    except:
        print('cannot create query')
    
    db = client['YoutubeData']
    collection2 =db["ChannelDetails"]
    list_cmt = []

    for cmt_dt in collection2.find({},{"_id":0,"CommentDetails":1}):
        for i in range(len(cmt_dt["CommentDetails"])):
            list_cmt.append(cmt_dt["CommentDetails"][i])
    df2 = pd.DataFrame(list_cmt)

    for index,row in df2.iterrows():
        query_insert = '''insert into comments(CommentId,
                                                videoID,
                                                CommentText,
                                                CommentAuthor,
                                                CommentPublishedAt)
                                            
                                            values(%s,%s,%s,%s,%s)'''
        values = (row['CommentId'],
                row['videoID'],
                row['CommentText'],                
                row['CommentAuthor'],
                row['CommentPublishedAt']
                )
        
        cursor_db.execute(query_insert,values)  
        mydatabase.commit()

    

    
def tables():
    table_channel()
    table_video()
    table_comment()

    return 'tables created'
            
            

#channel dataframe:
def views_channel():
    db = client['YoutubeData']
    collection2 =db["ChannelDetails"]
    list_ch = []

    for ch_dt in collection2.find({},{"_id":0,"channelInformation":1}):
        list_ch.append(ch_dt["channelInformation"])
    df = st.dataframe(list_ch)
    return df


def views_video():
    db = client['YoutubeData']
    collection2 =db["ChannelDetails"]
    list_vd = []

    for vd_dt in collection2.find({},{"_id":0,"VideoDetails":1}):
        for i in range(len(vd_dt["VideoDetails"])):
            list_vd.append(vd_dt["VideoDetails"][i])
    df1 = st.dataframe(list_vd)
    return df1    

def views_comment():
    db = client['YoutubeData']
    collection2 =db["ChannelDetails"]
    list_cmt = []

    for cmt_dt in collection2.find({},{"_id":0,"CommentDetails":1}):
        for i in range(len(cmt_dt["CommentDetails"])):
            list_cmt.append(cmt_dt["CommentDetails"][i])
    df2 = st.dataframe(list_cmt)

    return df2



with st.sidebar:
    st.title(":rainbow[Youtube Data Harvesting and Warehousing using SQL, MongoDB and Streamlit]")
    st.header("skill take away")
    st.caption("python scripting")
    st.caption("MongoDB")

channel_ID = st.text_input("Enter the channel ID")
st.caption("click on the channel and 'share channel' to get the channel id")

if st.button(":rainbow[collect and store data]"):
    ID_channel = []
    db = client["YoutubeData"]
    collection3 = db["ChannelDetails"]
    for ch_data in collection3.find({},{"_id":0,"channelInformation":1}):
        ID_channel.append(ch_data["channelInformation"]["channel_id"])

    if channel_ID in ID_channel:
        st.success("Details of this channel already exists")
    else:
        insert = channel_details(channel_ID)
        st.success(insert)

if st.button(":rainbow[Transfer to SQL]"):
    table = tables()
    st.success(table)

show_tables = st.radio("Tables: ",(":red[Channels]",":blue[Videos]",':orange[Comments]'))

if show_tables == ':red[Channels]':
    views_channel()

elif show_tables == ':blue[Videos]':
    views_video()

elif show_tables ==':orange[Comments]':
    views_comment()

#SQL QUERIES:

mydatabase = psycopg2.connect(host = "localhost",
                        user = "postgres",
                        password = 'manumanu',
                        database = "YoutubeData",
                        port = "5432")

cursor_db  = mydatabase.cursor()

queries = st.selectbox("Questions: ",("1] What are the names of all the videos and their corresponding channels?",
                                      "2] Which channels have the most number of videos, and how many videos do they have?",
                                      "3] What are the top 10 most viewed videos and their respective channels?",
                                      "4] How many comments were made on each video, and what are their corresponding video names? ",
                                      "5] Which videos have the highest number of likes, and what are their corresponding channel names? ",
                                      "6] What is the total number of likes for each video, and what are their corresponding video names? ",
                                      "7] What is the total number of views for each channel, and what are their corresponding channel names?",
                                      "8] What are the names of all the channels that have published videos in the year 2022? ",
                                      "9] What is the average duration of all videos in each channel, and what are their corresponding channel names? ",
                                      "10] Which videos have the highest number of comments, and what are their corresponding channel names? "
                                        ))
if queries == "1] What are the names of all the videos and their corresponding channels?":

    q1 = '''select channel_name as Channel,videoname from video'''
    cursor_db.execute(q1)
    mydatabase.commit()

    table = cursor_db.fetchall()
    df3 = pd.DataFrame(table,columns=['Channel','video name'])
    st.write(df3)

elif queries == "2] Which channels have the most number of videos, and how many videos do they have?":
    q2 = '''select channel_name as Channel,count(videoid) as video_count from video group by Channel order by video_count desc'''
    cursor_db.execute(q2)
    mydatabase.commit()
    table = cursor_db.fetchall()
    df3 = pd.DataFrame(table,columns=['Channel','Video Count'])
    st.write(df3)

elif queries == "3] What are the top 10 most viewed videos and their respective channels?":
    q3 = '''select channel_name as Channel,videoname,viewcount from video order by viewcount desc limit 10''' 
    cursor_db.execute(q3)
    mydatabase.commit()
    table = cursor_db.fetchall()
    df3 = pd.DataFrame(table,columns=['Channel','video name','View Count'])
    st.write(df3)

elif queries == "4] How many comments were made on each video, and what are their corresponding video names?":

    q4 = '''select videoname, sum(commentcount) as Totalcommentcount from video group by videoname'''
    cursor_db.execute(q4)
    mydatabase.commit()

    table = cursor_db.fetchall()
    df3 = pd.DataFrame(table,columns=['video name','Comment Count'])
    st.write(df3)

elif queries == "5] Which videos have the highest number of likes, and what are their corresponding channel names? ":
    q5 = '''select channel_name as Channel, videoname, likecount from video where likecount IS NOT NULL order by likecount desc'''
    cursor_db.execute(q5)
    mydatabase.commit()
    table = cursor_db.fetchall()
    df3 = pd.DataFrame(table,columns=['Channel','Video Name','Like Count'])
    st.write(df3)
     
elif queries == "6] What is the total number of likes for each video, and what are their corresponding video names? ":
    q6 = '''select channel_name as Channel, videoname, likecount from video where likecount IS NOT NULL '''
    cursor_db.execute(q6)
    mydatabase.commit()
    table = cursor_db.fetchall()
    df3 = pd.DataFrame(table,columns=['Channel','video name','Like Count'])
    st.write(df3)

elif queries == "7] What is the total number of views for each channel, and what are their corresponding channel names?":
    q7 = '''select channel_name as Channel,sum(viewcount) as Total_ViewCount from video group by Channel'''
    cursor_db.execute(q7)
    mydatabase.commit()
    table = cursor_db.fetchall()
    df3 = pd.DataFrame(table,columns=['Channel','Total View Count'])
    st.write(df3)

elif queries == "8] What are the names of all the channels that have published videos in the year 2022? ":
    q8 = '''select channel_name as Channel, videoname, publishedat from video where extract(year from publishedat) = 2022''' 
    cursor_db.execute(q8)
    mydatabase.commit()
    table = cursor_db.fetchall()
    df3 = pd.DataFrame(table,columns=['Channel','Video Name','Published At'])
    st.write(df3)

elif queries == "9] What is the average duration of all videos in each channel, and what are their corresponding channel names? ":
    q9 = '''select channel_name as Channel, avg(duration) as Average_Duration from video group by Channel''' 
    cursor_db.execute(q9)
    mydatabase.commit()
    table = cursor_db.fetchall()
    df3 = pd.DataFrame(table,columns=['Channel','AverageDuration'])  

    t9 = []
    for index, row in df3.iterrows():
        channel = row["Channel"]
        avg_duration = row["AverageDuration"]
        avg_dur_str = str(avg_duration)
        t9.append(dict(Channel = channel,AverageDuration = avg_dur_str ))
    df91 = pd.DataFrame(t9)
    st.write(df91)

elif queries == "10] Which videos have the highest number of comments, and what are their corresponding channel names? ":
    
    q6 = '''select videoname , channel_name, commentcount from video where commentcount is not null order by commentcount desc '''
    cursor_db.execute(q6)
    mydatabase.commit()
    table = cursor_db.fetchall()
    df3 = pd.DataFrame(table,columns=['Video Name','Channel','Comments Count'])
    st.write(df3)






