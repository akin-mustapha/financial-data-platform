resource "aws_sns_topic" "pipeline_alerts" {
  name = "financial-dataflow-alerts"
}

resource "aws_sns_topic_subscription" "pipeline_alerts_email" {
  topic_arn = aws_sns_topic.pipeline_alerts.arn
  protocol  = "email"
  endpoint  = "akinkunmimustapha1@gmail.com"
}

resource "aws_sns_topic_policy" "pipeline_alerts" {
  arn = aws_sns_topic.pipeline_alerts.arn

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "AllowLambdaDestination"
        Effect    = "Allow"
        Principal = { Service = "lambda.amazonaws.com" }
        Action    = "SNS:Publish"
        Resource  = aws_sns_topic.pipeline_alerts.arn
        Condition = {
          ArnLike = { "aws:SourceArn" = aws_lambda_function.pipeline["ingestion"].arn }
        }
      },
      {
        Sid       = "AllowCloudWatchAlarm"
        Effect    = "Allow"
        Principal = { Service = "cloudwatch.amazonaws.com" }
        Action    = "SNS:Publish"
        Resource  = aws_sns_topic.pipeline_alerts.arn
      }
    ]
  })
}

resource "aws_cloudwatch_metric_alarm" "ingestion_errors" {
  alarm_name          = "financial-dataflow-ingestion-errors"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = 300
  statistic           = "Sum"
  threshold           = 0
  alarm_description   = "Fires when financial-dataflow-ingestion has any invocation errors"
  treat_missing_data  = "notBreaching"

  dimensions = {
    FunctionName = aws_lambda_function.pipeline["ingestion"].function_name
  }

  alarm_actions = [aws_sns_topic.pipeline_alerts.arn]
}
