from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Filter,
    FilterExpression,
    FilterExpressionList,
    Metric,
    RunReportRequest,
)

def sample1(property_id):
    return RunReportRequest(
        property=f"properties/{property_id}",
            dimensions=[
                Dimension(name="pagePath"), # ページ
                Dimension(name="date"), # 日付
                Dimension(name="sessionSource"), # 参照元
                Dimension(name="sessionMedium"), # メディア
            ],
            metrics=[
                Metric(name="screenPageViews"), # ページビュー
                Metric(name="activeUsers"), # ユーザー数
                Metric(name="sessions"), # セッション数
                Metric(name="engagedSessions"), # エンゲージメントのあったセッション数
                Metric(name="averageSessionDuration"), # ページ滞在時間
                Metric(name='userEngagementDuration'), # ユーザーエンゲージメント
            ],
            date_ranges=[DateRange(start_date="2020-03-31", end_date="today")], # 日付範囲
            # dimension_filter=FilterExpression(
            #     filter=Filter(
            #         field_name="",
            #         string_filter=Filter.StringFilter(match_type='PARTIAL_REGEXP', value=''),
            #     )
    )