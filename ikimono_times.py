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


# 動画のIDから各動画の情報を取得（リクエスト条件をパラメータにセットし実行、取得した情報を取り出しまとめる）
def videos_info(ids_data, service):
    videos = []

    for video_id in ids_data:
      video = service.videos().list(
          id=video_id,
          part='snippet, contentDetails, statistics',
      ).execute()

      snippet = video['items'][0]['snippet']
      details = video['items'][0]['contentDetails']
      statistics = video['items'][0]['statistics']

      duration = isodate.parse_duration(details['duration'])

      videos.append({
          'id': video['items'][0]['id'],
          'title': snippet['title'],
          'description': snippet['description'],
          'published_at': snippet['publishedAt'],
          'duration': int(duration.total_seconds()),
          'view_count': int(statistics['viewCount']),
          'like_count': int(statistics['likeCount']),
          'dislike_count': int(statistics['dislikeCount']),
          'comment_count': statistics['commentCount']
      })
      
    return videos

# メイン関数を作成
def main():
    # 認証フローを作成 
    flow = flow_from_clientsecrets(SECRET_FILE, scope=CLIENT_SCOPES)

    # 資格情報を取得してファイルに保存
    storage = Storage('credentials.json')
    credentials = storage.get()
    if credentials is None or credentials.invalid:
        credentials = run_flow(flow, storage)

    # サービスオブジェクトの作成
    service = build(SERVICE_NAME, VERSION, http=credentials.authorize(httplib2.Http()))
    
    
    # 各情報を取得する関数の呼び出し
    channel_data = channel_info(service)
    ids_data = ids_info(service)
    videos_data = videos_info(ids_data, service)
    
    
    # 取得した動画情報をデータフレーム化しcsvで掃き出し
    data_file = pd.DataFrame(videos_data)
    data_file.to_csv('data_file.csv', index=False)
    
    
    # videos_dataから各動画の「高評価」、「低評価」、「総時間」を加算し総数を計算しその他情報とともに辞書化
    total = {
        'duration': 0,
        'like_count': 0,
        'dislike_count': 0
    }

    for video in videos_data:
        total['duration'] += video['duration']
        total['like_count'] += video['like_count']
        total['dislike_count'] += video['dislike_count']
        
    total_hours = total['duration'] / 60 /60  
    
    base_data = {'チャンネルタイトル': channel_data['title'],
                 '配信回数': channel_data['video_count'],
                 '総視聴回数': channel_data['view_count'],
                 'チャンネル登録者数': channel_data['subscriber_count'],
                 '総高評価数': total['like_count'],
                 '総低評価数': total['dislike_count'],
                 '総配信時間': math.floor(total_hours)}
    
    
    # 辞書化基本の情報をデータフレーム化し、csvで掃き出し
    base_data_file = pd.DataFrame.from_dict(base_data, orient = "index",columns=["data"])
    base_data_file.to_csv('base_data_file.csv')
    

if __name__ == '__main__':
    main()