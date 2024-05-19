import httplib2
import isodate
import math
import pandas as pd

from googleapiclient.discovery import build
from oauth2client.client import flow_from_clientsecrets
from oauth2client.file import Storage
from oauth2client.tools import run_flow

# OAuthクライエントIDなどを記載しているjsonファイルを変数にセット
SECRET_FILE = '/Users/kii/work/access_analysis/json/youtube_client_secret.json'

# 利用するAPIのスコープを指定
CLIENT_SCOPES = [
    'https://www.googleapis.com/auth/youtube.readonly'
]

# 利用するサービスとバージョンのセット
SERVICE_NAME = 'youtube'
VERSION = 'v3'
SERVICE_NAME_ANALYTICS = 'youtubeAnalytics'
VERSION_ANALYTICS = 'v2'

START_DATE = '2024-03-28'  # データ取得の開始日
END_DATE = '2024-05-17'    # データ取得の終了日

# メイン関数を作成
def main():
  
    youtube_analytics = get_authenticated_service(SERVICE_NAME, VERSION)

    # 各情報を取得する関数の呼び出し
    channel_data = channel_info(youtube_analytics)
    ids_data = ids_info(youtube_analytics)
    
    youtube_analytics = get_authenticated_service(SERVICE_NAME_ANALYTICS, VERSION_ANALYTICS)
    videos_data = get_video_by_day(youtube_analytics, channel_data['id'], ids_data)
    
    videos_data.to_csv('data_file.csv', index=False)
      
    base_data = {'チャンネルタイトル': channel_data['title'],
                 '配信回数': channel_data['video_count'],
                 '総視聴回数': channel_data['view_count'],
                 'チャンネル登録者数': channel_data['subscriber_count'],
                }
    
    # 辞書化基本の情報をデータフレーム化し、csvで掃き出し
    base_data_file = pd.DataFrame.from_dict(base_data, orient = "index",columns=["data"])
    base_data_file.to_csv('base_data_file.csv')


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
def channel_info(service):
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
def ids_info(service):
    search_request = service.search().list(
        part='id',
        forMine = True,
        type = 'video',
        order = 'date',
        maxResults = 50,
    )

    video_ids = []
    while search_request:
        res = search_request.execute()

        for video in res['items']:
            video_ids.append(video['id']['videoId'])

        search_request = service.search().list_next(
            previous_request = search_request,
            previous_response = res
        )
    return video_ids
  

# 日付ごとの動画情報を取得
def get_video_by_day(youtube_analytics, channel_id, video_ids):
  
  df = []
  for video_id in video_ids:
    request = youtube_analytics.reports().query(
        ids='channel=={}'.format(channel_id),
        startDate=START_DATE,
        endDate=END_DATE,
        metrics='views,estimatedMinutesWatched,averageViewDuration,likes,dislikes,comments,subscribersGained,subscribersLost',
        dimensions='day,video',
        sort='day,video',
        filters='video=={}'.format(video_id),
    )
    response = request.execute()
    # responseの内容をPandas DataFrameに変換してdfリストに追加
    df.append(pd.DataFrame(response['rows'], columns=['day', 'video_id', 'views', 'estimatedMinutesWatched', 'averageViewDuration', 'likes', 'dislikes', 'comments', 'subscribersGained', 'subscribersLost']))
    
    # 最終的に１つの表にするために、全てのデータフレームを連結
    final_df = pd.concat(df, ignore_index=True)

  return final_df
    

if __name__ == '__main__':
    main()