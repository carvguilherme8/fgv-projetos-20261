# ---------------------------------------------------------------------------
# EventBridge Rule — Scheduled trigger for the Glue ETL job
# ---------------------------------------------------------------------------
# Fires weekly on Mondays at 12:00 UTC.
# Adjust the cron expression as needed.
# Cron format: cron(minutes hours day-of-month month day-of-week year)

resource "aws_cloudwatch_event_rule" "glue_schedule" {
  name                = "${var.project_name}-etl-schedule"
  description         = "Weekly trigger for the incremental ETL Glue job"
  schedule_expression = "cron(0 12 ? * MON *)"
}

# ---------------------------------------------------------------------------
# EventBridge Target — Starts the Glue Job
# ---------------------------------------------------------------------------
# NOTE: In AWS Academy labs the LabRole already has the necessary permissions
# (glue:StartJobRun). If running in a different environment, ensure the
# EventBridge role has the glue:StartJobRun permission on the target job.

resource "aws_cloudwatch_event_target" "glue_job" {
  rule     = aws_cloudwatch_event_rule.glue_schedule.name
  arn      = aws_glue_job.etl.arn
  role_arn = data.aws_iam_role.lab_role.arn

  # Pass the Glue job name for the StartJobRun API call
  input = jsonencode({
    JobName = aws_glue_job.etl.name
  })
}
