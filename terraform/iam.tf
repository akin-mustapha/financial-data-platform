resource "aws_iam_role" "financial_dataflow" {
  name = "financial-dataflow-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = [
            "glue.amazonaws.com",
            "scheduler.amazonaws.com"
          ]
        }
        Action = "sts:AssumeRole"
        Condition = {
          StringEquals = { "aws:SourceAccount" = var.aws_account_id }
        }
      }
    ]
  })
}

resource "aws_iam_role_policy" "financial_dataflow" {
  name = "financial-dataflow-policy"
  role = aws_iam_role.financial_dataflow.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "S3"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.financial_dataflow.arn,
          "${aws_s3_bucket.financial_dataflow.arn}/*",
          "arn:aws:s3:::aws-glue-assets-${var.aws_account_id}-${var.aws_region}",
          "arn:aws:s3:::aws-glue-assets-${var.aws_account_id}-${var.aws_region}/*"
        ]
      },

      {
        Sid = "Lambda"
        Effect = "Allow"
        Action = [
          "lambda:InvokeFunction"
        ],
        Resource = [
          aws_lambda_function.pipeline["ingestion"].arn
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "glue_service_role" {
  role       = aws_iam_role.financial_dataflow.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# Separate from financial_dataflow: Lambda Destinations requires the
# invoking function's execution role to trust only lambda.amazonaws.com
# ("Role trusts too many services" if it's shared with Glue/Scheduler),
# so this role cannot be folded into the shared one above.
resource "aws_iam_role" "lambda_pipeline" {
  name = "financial-dataflow-lambda-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Principal = { Service = "lambda.amazonaws.com" }
        Action    = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_role_policy" "lambda_pipeline" {
  name = "financial-dataflow-lambda-policy"
  role = aws_iam_role.lambda_pipeline.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "CloudWatchLogs"
        Effect   = "Allow"
        Action   = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = ["arn:aws:logs:${var.aws_region}:${var.aws_account_id}:log-group:/aws/lambda/*"]
      },
      {
        Sid      = "Secrets"
        Effect   = "Allow"
        Action   = ["secretsmanager:GetSecretValue"]
        Resource = [
          aws_secretsmanager_secret.trading212.arn,
          "arn:aws:secretsmanager:eu-west-1:861580917950:secret:prod/financial/t212-dca-automation-UqUvlT"
          ]
      },
      {
        Sid      = "S3Bucket"
        Effect   = "Allow"
        Action   = [
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ]
        Resource = [aws_s3_bucket.financial_dataflow.arn]
      },
      {
        Sid      = "S3Objects"
        Effect   = "Allow"
        Action   = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject"
        ]
        Resource = ["${aws_s3_bucket.financial_dataflow.arn}/*"]
      },
      {
        Sid      = "Glue"
        Effect   = "Allow"
        Action   = [
          "glue:GetDatabase",
          "glue:GetTable",
          "glue:CreateTable",
          "glue:UpdateTable",
          "glue:GetPartitions",
          "glue:CreatePartition",
          "glue:BatchCreatePartition",
          "glue:BatchGetPartition",
          "glue:UpdatePartition",
          "glue:DeleteTable"
        ]
        Resource = [
          "arn:aws:glue:${var.aws_region}:${var.aws_account_id}:catalog",
          "arn:aws:glue:${var.aws_region}:${var.aws_account_id}:database/financials",
          "arn:aws:glue:${var.aws_region}:${var.aws_account_id}:database/default",
          "arn:aws:glue:${var.aws_region}:${var.aws_account_id}:table/financials/*"
        ]
      },
      {
        Sid      = "Athena"
        Effect   = "Allow"
        Action   = [
          "athena:GetWorkGroup",
          "athena:StartQueryExecution",
          "athena:GetQueryExecution",
          "athena:GetQueryResults",
          "athena:StopQueryExecution",
          "athena:GetDataCatalog"
        ]
        Resource = [
          "arn:aws:athena:${var.aws_region}:${var.aws_account_id}:workgroup/*",
          "arn:aws:athena:${var.aws_region}:${var.aws_account_id}:datacatalog/*"
        ]
      },
      {
        Sid      = "PublishFailureAlerts"
        Effect   = "Allow"
        Action   = ["sns:Publish"]
        Resource = [aws_sns_topic.pipeline_alerts.arn]
      },
      {
        Sid      = "DynamoDb"
        Effect   = "Allow"
        Action   = [
          "dynamodb:GetItem",
          "dynamodb:PutItem",
          "dynamodb:BatchGetItem",
          "dynamodb:BatchWriteItem",
          "dynamodb:Query",
          "dynamodb:Scan"
        ]
        Resource = [
          aws_dynamodb_table.trade_log.arn  # Or "arn:aws:dynamodb:${var.aws_region}:${var.aws_account_id}:table/trade_log"
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_basic_execution" {
  role       = aws_iam_role.lambda_pipeline.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}