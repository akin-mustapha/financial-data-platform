resource "aws_scheduler_schedule" "europe_open" {
  name       = "financial-dataflow-europe-open"
  group_name = "default"

  flexible_time_window {
    mode = "OFF"
  }

  schedule_expression          = "cron(20 8 * * ? *)"
  schedule_expression_timezone = "Europe/Dublin"

  target {
    arn      = aws_lambda_function.pipeline["ingestion"].arn
    role_arn = aws_iam_role.financial_dataflow.arn
    input    = jsonencode({ run_type = "europe-open" })

    retry_policy {
      maximum_event_age_in_seconds = 86400
      maximum_retry_attempts       = 0
    }
  }
}

resource "aws_scheduler_schedule" "midday" {
  name       = "financial-dataflow-midday"
  group_name = "default"

  flexible_time_window {
    mode = "OFF"
  }

  schedule_expression          = "cron(20 13 * * ? *)"
  schedule_expression_timezone = "Europe/Dublin"

  target {
    arn      = aws_lambda_function.pipeline["ingestion"].arn
    role_arn = aws_iam_role.financial_dataflow.arn
    input    = jsonencode({ run_type = "midday" })

    retry_policy {
      maximum_event_age_in_seconds = 86400
      maximum_retry_attempts       = 0
    }
  }
}

resource "aws_scheduler_schedule" "us_close" {
  name       = "financial-dataflow-us-close"
  group_name = "default"

  flexible_time_window {
    mode = "OFF"
  }

  schedule_expression          = "cron(30 16 * * ? *)"
  schedule_expression_timezone = "Europe/Dublin"

  target {
    arn      = aws_lambda_function.pipeline["ingestion"].arn
    role_arn = aws_iam_role.financial_dataflow.arn
    input    = jsonencode({ run_type = "us-close" })

    retry_policy {
      maximum_event_age_in_seconds = 86400
      maximum_retry_attempts       = 0
    }
  }
}
