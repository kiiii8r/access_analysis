# ライブラリのインポート
import ast
import boto3
import base64
import pandas as pd
import isodate
from datetime import datetime, timedelta
from googleapiclient.discovery import build
from oauth2client.client import flow_from_clientsecrets
from oauth2client.file import Storage
from oauth2client.tools import run_flow

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import get_as_dataframe, set_with_dataframe

# OAuthクライエントIDなどを記載しているjsonファイルを変数にセット
SECRET_FILE = '/Users/kii/work/access_analysis/json/youtube_client_secret.json'

# 利用するAPIのスコープを指定
CLIENT_SCOPES = [
    'https://www.googleapis.com/auth/youtube.readonly'
]

# 利用するサービスとバージョンのセット
SERVICE_NAME = 'youtube' # YouTube Data API v3
VERSION = 'v3'
SERVICE_NAME_ANALYTICS = 'youtubeAnalytics' # YouTube Analytics API v2
VERSION_ANALYTICS = 'v2'

# 日付の設定
current_date = datetime.now() # 現在の日付を取得
ch_open_date = datetime(2024, 3, 28) # チャンネル開設日

# チャンネル開設日が1年以上前の場合、1年前の日付から取得。
if (current_date - ch_open_date).days >= 365:
    START_DATE = (current_date - timedelta(days=365)).date().isoformat()
else:
    START_DATE = '2024-03-28'
END_DATE = datetime.now().date().isoformat()  # 最新の日付まで取得


def lambda_handler(event, context):
    service_data = get_authenticated_service(SERVICE_NAME, VERSION)  # YouTube Data API v3 を使用
    service_analytics = get_authenticated_service(SERVICE_NAME_ANALYTICS, VERSION_ANALYTICS)  # YouTube Analytics API v2 を使用 

    # チャンネル情報を取得
    df_channel = get_channel_info(service_data)
    
    # 動画IDリスト取得
    video_id_list = get_video_id_list(service_data)
    
    # 動画情報を取得
    df_video_info = get_video_info(service_data, video_id_list)
    
    # 日付ごとの動画情報を取得
    df_date_video = get_video_by_day(service_analytics, df_channel['id'], video_id_list)
    
    # df_video, df_video_info, df_date_videoを結合するコード
    df_video = df_video_info.merge(df_date_video, on='id', how='inner')
    
    # スプシの更新
    update_spread_sheet(df_video)
    
    print('処理完了')


def get_authenticated_service(service_name, version):
    # 認証フローを作成 
    flow = flow_from_clientsecrets(SECRET_FILE, scope=CLIENT_SCOPES)

    # 資格情報を取得してファイルに保存
    storage = Storage('credentials.json')
    credentials = storage.get()
    if credentials is None or credentials.invalid:
        credentials = run_flow(flow, storage)

    # サービスオブジェクトの作成
    service = build(service_name, version, http=credentials.authorize(httplib2.Http()))
    
    return service


# チャンネル情報を取得する関数を作成（リクエスト条件をパラメータにセットし実行、取得した情報を取り出しまとめる）
def get_channel_info(service):
    channels = service.channels().list(
        part = 'snippet, statistics',
        mine = True
    ).execute()

    channel = channels['items'][0]
    snippet = channel['snippet']
    statistics = channel['statistics']
    return {
            'id': channel['id'],
            'title': snippet['title'],
            'description': snippet['description'],
            'video_count': statistics['videoCount'],
            'view_count': statistics['viewCount'],
            'subscriber_count': statistics['subscriberCount']
        }


# 動画のidを取得する関数の作成（リクエスト条件をパラメータにセットし実行。取得した情報を取り出しまとめる）
def get_video_id_list(service):
    videos = service.search().list(
        part='snippet',
        forMine=True,
        type='video',
        order='date',
        maxResults=50,
    ).execute()

    video_id_list = []
    
    for video in videos['items']:
        video_id_list.append(video['id']['videoId'])
        
    return video_id_list


# ビデオの詳細情報を取得
def get_video_info(service, video_id_list):
  
  video_details_list = []
  for video_id in video_id_list:
    video_details = service.videos().list(
        id=video_id,
        part='snippet,contentDetails',
    ).execute()

    for video in video_details['items']:
        video_info = {
            'title': video['snippet']['title'],
            'id': video['id'],
            'duration': isodate.parse_duration(video['contentDetails']['duration']).total_seconds(),
            'definition': video['contentDetails']['definition']
        }
        video_details_list.append(video_info)

  df_video_details = pd.DataFrame(video_details_list)
  
  return df_video_details
    

# 日付ごとの動画情報を取得
def get_video_by_day(service, channel_id, video_id_list):
  
  date_video_list = []
  for video_id in video_id_list:
    response = service.reports().query(
        ids='channel=={}'.format(channel_id),
        startDate=START_DATE,
        endDate=END_DATE,
        metrics='views,estimatedMinutesWatched,averageViewDuration,likes,dislikes,comments,subscribersGained,subscribersLost',
        dimensions='day,video',
        sort='day,video',
        filters='video=={}'.format(video_id),
    ).execute()
    
    # responseの内容をPandas DataFrameに変換してdfリストに追加
    date_video_list.append(pd.DataFrame(response['rows'], columns=['day', 'id', 'views', 'estimatedMinutesWatched', 'averageViewDuration', 'likes',
                                                                   'dislikes', 'comments', 'subscribersGained', 'subscribersLost']))
    
  # 全てのデータフレームを連結
  df_date_video = pd.concat(date_video_list, ignore_index=True)
  
  # 合計試聴時間を算出し、平均試聴時間を削除
  df_date_video['sumViewDuration'] = df_date_video['averageViewDuration'] * df_date_video['views']
  df_date_video.drop(columns=['averageViewDuration'], inplace=True)
  
  return df_date_video


# スプレッドシートの更新
def update_spread_sheet(df):
  # スプレッドシート連携
  # creds を使って Google Drive API と対話するためのクライアントを作成
  scope =['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
  creds = ServiceAccountCredentials.from_json_keyfile_name('json/spread_key.json', scope)
  client = gspread.authorize(creds)

  try:
    # Google スプレッドシート "Youtube rawdata" を開き、"rawdata" シートを取得
    worksheet = client.open("Youtube rawdata").worksheet('rawdata')

    # シートの内容をクリア
    worksheet.clear()
    
    # DataFrame のデータをシートに転送（インデックスはリセット）
    set_with_dataframe(worksheet, df.reset_index(drop=True))
    
    print("スプレッドシートの更新が完了しました。")
  except Exception as e:
    print(f"スプレッドシートの更新中にエラーが発生しました: {e}")
  
  return