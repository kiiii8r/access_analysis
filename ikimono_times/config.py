from datetime import datetime, timedelta

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