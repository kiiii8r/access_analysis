import json
import pandas as pd
from google.oauth2 import service_account
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Filter,
    FilterExpression,
    FilterExpressionList,
    Metric,
    RunReportRequest,
)
from request import ga4_request


def main():

    # データを取得
    df = get_ga4()
    
    print(df) 


# GA4のデータを取得・整形
def get_ga4():
    
    # 認証情報を読み込む
    credentials = service_account.Credentials.from_service_account_file('python_spread.json')

    # クライアントを作成
    client = BetaAnalyticsDataClient(credentials=credentials)
    
    # プロパティIDを設定
    with open('ga4.json') as f:
        data = json.load(f)
    property_id = data['property_id']

    response = client.run_report(ga4_request.sample1(property_id))
    
    # データを整形
    data = []
    for row in response.rows:
        data.append([dimension.value for dimension in row.dimension_values] + [metric.value for metric in row.metric_values])

    # データフレームを作成
    df = pd.DataFrame(data, columns=[dimension.name for dimension in response.dimension_headers] + [metric.name for metric in response.metric_headers])
    
    return df


if __name__ == '__main__':
    main()