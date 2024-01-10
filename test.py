from pymongo import MongoClient
import pymongo
import streamlit as st
import pandas as pd


#API key connection



import googleapiclient.discovery
import googleapiclient.errors

api_service_name = "youtube"
api_version = "v3"
    

    # Get credentials and create an API client
    
    
youtube = googleapiclient.discovery.build(
        api_service_name, api_version, developerKey="AIzaSyA7PxbFAFtxgVl87tTWKZQlKHXk2E7gvy8")
 

#get channel info
def get_channel_info(channel_id):
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id
    )
    response = request.execute()

    for i in response['items']:
        data=dict(channel_Name=i["snippet"]["title"],
              channel_Id=i["id"],Subscribers=i['statistics']["subscriberCount"],
              Views=i["statistics"]["viewCount"],
              Total_videos=i["statistics"]["videoCount"],
              channel_Description=i["snippet"]["description"],
              Playlist_Id=i["contentDetails"]["relatedPlaylists"]["uploads"])
    return data


#get video ids
def get_ids(playlist_id):
    video_ids=[]
    next_page=None



    while True:
        request = youtube.playlistItems().list(part = 'snippet,contentDetails',playlistId=playlist_id,
                                                        maxResults=50,pageToken=next_page)
        response = request.execute()
        for item in response['items']:
            video_ids.append(item['contentDetails']['videoId'])
        next_page = response.get('nextPageToken')
        if next_page is None:
            break
    return video_ids

#get video info
def get_video_info(Video_ids):
    video_data=[]
    for video_id in Video_ids:
        request=youtube.videos().list(
            part='snippet,contentDetails,statistics',
            id=video_id
        )
        response=request.execute()
        for item in response ['items']:
            data=dict(channel_Name=item['snippet']['channelTitle'],
                    channel_Id=item['snippet']['channelId'],
                    video_Id=item['id'],
                    Title=item['snippet']['title'],
                    Tags= ','.join(item['snippet'].get('tags',['NA'])),
                    
                    

                    

                    Thumbnail=item['snippet']['thumbnails']['default']['url'],
                    Description=item['snippet'].get('description'),
                    Published_Date=item['snippet']['publishedAt'],
                    Duration=item['contentDetails']['duration'],
                    Views=item['statistics'].get('viewCount'),
                    Likes=item['statistics'].get('likeCount'),
                    Comments=item['statistics'].get('commentCount'),
                    Favorite_Count=item['statistics']['favoriteCount'],
                    Definition=item['contentDetails']['definition'],
                    Caption_Status=item['contentDetails']['caption']

                    
                    

                    )
            video_data.append(data)
    return  video_data
        
    
        
    
#get comment info
def get_comment_info(video_Ids):
    Comment_data=[]
    try:
        for video_id in video_Ids:
            request=youtube.commentThreads().list(
                part='snippet',
                videoId=video_id,
                maxResults=50
            )
            response=request.execute()

            for item in response['items']:
                data=dict(Comment_Id=item['snippet']['topLevelComment']['id'],
                        Video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                        Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                        Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        Comment_Published=item['snippet']['topLevelComment']['snippet']['publishedAt']
                        )
                Comment_data.append(data)
                
    except:
        pass
    return Comment_data


#get playlit details
def get_playlist_details(channel_id):
    next_page_token=None
    All_data=[]
    while True:
        request=youtube.playlists().list(
            part='snippet,contentDetails',
            channelId=channel_id,
            maxResults=50,
            pageToken=next_page_token
        )
        response=request.execute()

        for item in response['items']:
            data=dict(Playlist_Id=item['id'],
                    Title=item['snippet']['title'],
                    Channel_Id=item['snippet']['channelId'],
                    Channel_Name=item['snippet']['channelTitle'],
                    PublishedAt=item['snippet']['publishedAt'],
                    Video_Count=item['contentDetails']['itemCount'] 







                    )
            All_data.append(data)
        next_page_token=response.get('nextPageToken')
        if next_page_token is None:
            break
    return All_data
        

#upload to mongodb

client=pymongo.MongoClient("mongodb+srv://sangeethasam:6228@cluster0.imzvuad.mongodb.net/?retryWrites=true&w=majority"
)
db=client["youtube_data"]

def channel_details(channel_id):
    ch_details=get_channel_info(channel_id)
    pl_details=get_playlist_details(channel_id)
    vi_ids=get_ids(ch_details['Playlist_Id'])
    vi_details=get_video_info(vi_ids)
    com_details=get_comment_info(vi_ids)
    
    coll1=db["channel_details"]

    coll1.insert_one({"channel_information":ch_details,"playlist_information":pl_details,"video_information":vi_details,"comment_information":com_details})
    return "upload completed successfully"
    

import mysql.connector
mydb=mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database='project'




)
print(mydb)
mycursor=mydb.cursor(buffered=True)


# table creation for channels
import mysql.connector
def channels_table():
    mydb=mysql.connector.connect(
        host="localhost",
        user="root",
        password="",
        database='project'




    )
    print(mydb)
    mycursor=mydb.cursor(buffered=True)

    drop_query='''drop table if exists channels'''
    mycursor.execute(drop_query)
    mydb.commit()
    mycursor.execute("CREATE TABLE project.channels (channel_name VARCHAR (100),channel_id VARCHAR (80) Primary key,subscribers BIGINT,views BIGINT,total_videos INT,channel_description TEXT,playlist_id VARCHAR (80))")
    mydb.commit()


    ch_list=[]
    db=client["youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])

    df=pd.DataFrame(ch_list)


    for index,row in df.iterrows():
        insert_query='''insert into project.channels(channel_name,channel_id,subscribers,views,total_videos,channel_description,playlist_Id)
                                            values(%s,%s,%s,%s,%s,%s,%s)'''
        values=(row['channel_Name'],
                row['channel_Id'],
                row['Subscribers'],
                row['Views'],
                row['Total_videos'],
                row['channel_Description'],
                row['Playlist_Id'])
                    
        mycursor.execute(insert_query,values)
        mydb.commit()
    
        
        
        

def playlists_table():
     
    mydb=mysql.connector.connect(
            host="localhost",
            user="root",
            password="",
            database='project'




        )
    print(mydb)
    mycursor=mydb.cursor(buffered=True)

    drop_query='''drop table if exists playlists'''
    mycursor.execute(drop_query)
    mydb.commit()
    mycursor.execute("CREATE TABLE project.playlists (Playlist_Id VARCHAR (100) Primary key,Title VARCHAR (100) ,Channel_Id VARCHAR(100),Channel_Name VARCHAR(100),PublishedAt TIMESTAMP, Video_Count INT)")
    mydb.commit()
    pl_list=[]
    db=client["youtube_data"]
    coll1=db["channel_details"]
    for pl_data in coll1.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(pl_data["playlist_information"])):
            pl_list.append(pl_data["playlist_information"][i])
    df1=pd.DataFrame(pl_list)
    for index,row in df1.iterrows():
            insert_query='''insert into project.playlists(Playlist_Id,Title,Channel_Id,Channel_Name,PublishedAt,Video_Count)
                                                values(%s,%s,%s,%s,%s,%s)'''
            values=(row['Playlist_Id'],
                    row['Title'],
                    row['Channel_Id'],
                    row['Channel_Name'],
                    row['PublishedAt'],
                    row['Video_Count']
                    )
            

            
            mycursor.execute(insert_query,values)
            mydb.commit()

def videos_table():

    mydb=mysql.connector.connect(
            host="localhost",
            user="root",
            password="",
            database='project'




        )
    print(mydb)
    mycursor=mydb.cursor(buffered=True)

    drop_query='''drop table if exists videos'''
    mycursor.execute(drop_query)
    mydb.commit()
    mycursor.execute("CREATE TABLE videos  (channel_Name varchar(100),channel_Id varchar(100),video_Id varchar(30) primary key,Title varchar(150),Tags text,Thumbnail varchar(200),Description text,Published_Date timestamp,Duration int,Views bigint,Likes bigint,Comments int,Favorite_Count int,Definition varchar(10),Caption_Status varchar(50))")
                                
    mydb.commit()
    vi_list=[]
    db=client["youtube_data"]
    coll1=db["channel_details"]
    for vi_data in coll1.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    df2=pd.DataFrame(vi_list)
    for index,row in df2.iterrows():
                insert_query='''insert into project.videos(channel_Name,channel_Id ,video_Id,Title,Tags ,Thumbnail ,Description ,Published_Date ,Duration ,Views ,Likes ,Comments ,Favorite_Count ,Definition ,Caption_Status )
                            
                                            values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
                values=(row['channel_Name'],
                        row['channel_Id'],
                        row['video_Id'],
                        row['Title'],
                        row['Tags'],
                        row['Thumbnail'],
                        row['Description'],
                        row['Published_Date'],
                        row['Duration'],
                        row['Views'],
                        row['Likes'],
                        row['Comments'],
                        row['Favorite_Count'],
                        row['Definition'],
                        row['Caption_Status']
                        )
                mycursor.execute(insert_query,values)
                mydb.commit()
        


    
                    
                


                    
        


def comments_table():

    mydb=mysql.connector.connect(
            host="localhost",
            user="root",
            password="",
            database='project'




        )
    print(mydb)
    mycursor=mydb.cursor(buffered=True)

    drop_query='''drop table if exists comments'''
    mycursor.execute(drop_query)
    mydb.commit()
    mycursor.execute("CREATE TABLE project.comments (Comment_Id varchar(100) primary key,Video_Id varchar(50),Comment_Text text,Comment_Author varchar(150),Comment_Published timestamp)")
    mydb.commit()

    com_list=[]
    db=client["youtube_data"]
    coll1=db["channel_details"]
    for com_data in coll1.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    df3=pd.DataFrame(com_list)


    for index,row in df3.iterrows():
                insert_query='''insert into project.comments(Comment_Id,Video_Id,Comment_Text,Comment_Author,Comment_Published)
                                                    values(%s,%s,%s,%s,%s)'''
                values=(row['Comment_Id'],
                        row['Video_Id'],
                        row['Comment_Text'],
                        row['Comment_Author'],
                        row['Comment_Published']
                        
                        )
                

                
                mycursor.execute(insert_query,values)
                mydb.commit()
    






def tables():
    channels_table()
    playlists_table()
    videos_table()
    comments_table()
    return 'Tables Created Successfully'

def show_channels_table(): 
    ch_list=[]
    db=client["youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])

    df=st.dataframe(ch_list)

    return df


def show_playlists_table():
    pl_list=[]
    db=client["youtube_data"]
    coll1=db["channel_details"]
    for pl_data in coll1.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(pl_data["playlist_information"])):
            pl_list.append(pl_data["playlist_information"][i])
    df1=st.dataframe(pl_list)

    return df1

def show_videos_table():
    vi_list=[]
    db=client["youtube_data"]
    coll1=db["channel_details"]
    for vi_data in coll1.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    df2=st.dataframe(vi_list)

    return df2

def show_comments_table():
    com_list=[]
    db=client["youtube_data"]
    coll1=db["channel_details"]
    for com_data in coll1.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    df3=st.dataframe(com_list)

    return df3

# streamlit part
with st.sidebar:
    st.title(":blue[Youtube Data Harvesting And Warehousing]")
    st.header("WELCOME")

channel_id=st.text_input("Enter the Youtube Channel id")

if st.button("collect and store data in MongoDB"):                      #This button is help to transfer the data into MongoDB 
    ch_ids=[]
    db=client["youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_ids.append(ch_data["channel_information"]["channel_Id"])

    if channel_id in ch_ids:                                           # To find out the Duplicate channel id (already exists in our database)
        st.success("channel details already exists")
    else:
        insert=channel_details(channel_id)
        st.success(insert)

if st.button("Migrate to Sql"):
    Table=tables()
    st.success(Table)

show_table=st.radio("SELECT THE TABLE FOR VIEW",("CHANNELS","PLAYLISTS","VIDEOS","COMMENTS"))

if show_table=="CHANNELS":
    show_channels_table()

elif show_table=="PLAYLISTS":
    show_playlists_table()

elif show_table=="VIDEOS":
    show_videos_table()

elif show_table=="COMMENTS":
    show_comments_table()
    


        


# SQL Connection
import mysql.connector
mydb=mysql.connector.connect(
        host="localhost",
        user="root",
        password="",
        database='project'




    )
print(mydb)
mycursor=mydb.cursor(buffered=True)
question=st.selectbox("Select your question",("1. All the videos and the channel name",
                                              "2. channels with most number of videos",
                                              "3. 10 most viewed videos",
                                              "4. comments in each video",
                                              "5. videos with highest likes",
                                              "6. likes of all videos",
                                              "7. views of each channel",
                                              "8. videos published in the year of 2022",
                                              "9. average duration of all videos in each channel",
                                              "10. videos with highest number of comments"))


if question=="1. All the videos and the channel name":
    q1='''select Title as videos,channel_Name as channelname from videos'''
    mycursor.execute(q1)
    mydb.commit()
    tab1=mycursor.fetchall()
    df=pd.DataFrame(tab1,columns=['video title','channel name'])
    st.write(df)

elif question=="2. channels with most number of videos":
    q2='''select channel_Name as channelname,Total_videos as num_videos from channels
                order by Total_videos desc'''
    mycursor.execute(q2)
    mydb.commit()
    tab2=mycursor.fetchall()
    df2=pd.DataFrame(tab2,columns=['channel name','num of videos'])
    st.write(df2)

elif question=="3. 10 most viewed videos":
    q3='''select Views as views,channel_Name as channelname,Title as videotitle from videos
            where views is not null order by Views desc limit 10'''
    mycursor.execute(q3)
    mydb.commit()
    tab3=mycursor.fetchall()
    df3=pd.DataFrame(tab3,columns=['views','channel name','videotitle'])
    st.write(df3)

elif question=="4. comments in each video":                                                                                    
    q4='''select Comments as no_comments,Title as videotitle from videos where Comments is not null'''
    mycursor.execute(q4)
    mydb.commit()
    tab4=mycursor.fetchall()
    df4=pd.DataFrame(tab4,columns=['no_comments','videotitle'])
    st.write(df4)

elif question=="5. videos with highest likes":                                                                                    
        q5='''select Title as videotitle,channel_Name as channelname,Likes as likecount
                from Videos where likes is not null order by likes desc'''
        mycursor.execute(q5)
        mydb.commit()
        tab5=mycursor.fetchall()
        df5=pd.DataFrame(tab5,columns=['videotitle','channelneme','likecount'])
        st.write(df5)
    
elif question=="6. likes of all videos":                                                                                    
        q6='''select Likes as likecount,Title as videotitle from Videos '''
        mycursor.execute(q6)
        mydb.commit()
        tab6=mycursor.fetchall()
        df6=pd.DataFrame(tab6,columns=['likecount','videotitle'])
        st.write(df6)
    
elif question=="7. views of each channel":                                                                                    
    q7='''select channel_name as channelname,views as totalviews from channels '''
    mycursor.execute(q7)
    mydb.commit()
    tab7=mycursor.fetchall()
    df7=pd.DataFrame(tab7,columns=['channel name','totalviews'])
    st.write(df7)

elif question=="8. videos published in the year of 2022":                                                                                    
    q8='''select Title as video_title,Published_Date as videorelease,channel_Name as channelname from videos
            where extract(Year from Published_Date)=2022'''
    mycursor.execute(q8)
    mydb.commit()
    tab8=mycursor.fetchall()
    df8=pd.DataFrame(tab8,columns=['videotitle','published_date','channelname'])
    st.write(df8)

elif question=="9. average duration of all videos in each channel":                                                                                    
    q9='''select channel_Name as channelname,AVG(Duration) as averageduration from videos group by channel_Name'''
    mycursor.execute(q9)
    mydb.commit()
    tab9=mycursor.fetchall()
    df9=pd.DataFrame(tab9,columns=['channelname','averageduration'])
    

    t9=[]
    for index,row in df9.iterrows():
        channel_title=row['channelname']
        average_duration=row['averageduration']
        averge_duration_str=str(average_duration)
        t9.append(dict(channeltitle=channel_title,avgduration=averge_duration_str))
    df1=pd.DataFrame(t9)
    st.write(df1)

elif question=="10. videos with highest number of comments":                                                                                    
    q10='''select Title as videotitle,channel_Name as channelname,Comments as comments from videos 
            where Comments is not null order by Comments desc'''
    mycursor.execute(q10)
    mydb.commit()
    tab10=mycursor.fetchall()
    df10=pd.DataFrame(tab10,columns=['video title','channel name','comments'])
    st.write(df10)

    







    


