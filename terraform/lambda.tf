locals {
  pipeline_stages = toset(["ingestion", "transformation", "processing"])
}

# ==========================================
# DATA PIPELINE LAMBDAS
# ==========================================

data "archive_file" "pipeline" {
  for_each         = local.pipeline_stages
  type             = "zip"
  source_file      = "${path.module}/../src/pipeline/t212-data-${each.key}.py"
  output_path      = "${path.module}/build/t212-data-${each.key}.zip"
  output_file_mode = "0644"
}

resource "aws_lambda_function" "pipeline" {
  for_each      = local.pipeline_stages
  function_name = "t212-data-${each.key}"
  role          = aws_iam_role.lambda_pipeline.arn

  handler       = "t212-data-${each.key}.lambda_handler"
  runtime       = "python3.12"
  timeout       = 300
  memory_size   = 512

  filename         = data.archive_file.pipeline[each.key].output_path
  source_code_hash = data.archive_file.pipeline[each.key].output_base64sha256

  layers = [
    "arn:aws:lambda:${var.aws_region}:336392948345:layer:AWSSDKPandas-Python312:13"
  ]

  depends_on = [
    aws_iam_role_policy.lambda_pipeline
  ]
}

resource "aws_lambda_function_event_invoke_config" "pipeline" {
  for_each      = local.pipeline_stages
  function_name = aws_lambda_function.pipeline[each.key].function_name

  destination_config {
    on_failure {
      destination = aws_sns_topic.pipeline_alerts.arn
    }
  }
}

# ==========================================
# DCA AUTOMATION LAMBDA
# ==========================================

data "archive_file" "t212_dca" {
  type             = "zip"
  source_file      = "${path.module}/../src/t212-dca.py"
  output_path      = "${path.module}/build/t212-dca.zip"
  output_file_mode = "0644"
}

resource "aws_lambda_function" "t212_dca" {
  function_name = "t212-dca"
  role          = aws_iam_role.lambda_pipeline.arn

  handler       = "t212-dca.lambda_handler"
  runtime       = "python3.12"
  timeout       = 300
  memory_size   = 512  # Recommended allocation for Pandas + Athena execution

  filename         = data.archive_file.t212_dca.output_path
  source_code_hash = data.archive_file.t212_dca.output_base64sha256

  layers = [
    "arn:aws:lambda:${var.aws_region}:336392948345:layer:AWSSDKPandas-Python312:13"
  ]

  depends_on = [
    aws_iam_role_policy.lambda_pipeline
  ]
}

# Failure Destination Alerting for DCA Lambda
resource "aws_lambda_function_event_invoke_config" "t212_dca" {
  function_name = aws_lambda_function.t212_dca.function_name

  destination_config {
    on_failure {
      destination = aws_sns_topic.pipeline_alerts.arn
    }
  }
}