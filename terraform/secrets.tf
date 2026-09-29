resource "aws_secretsmanager_secret" "trading212" {
  name = "prod/financial-dataflow/trading212"
}


# resource "aws_secretsmanager_secret" "t212_dca_automation" {
#   name = "prod/financial/t212-dca-automation"
# }
