# ---------------------------------------------------------------------------
# Glue Scheduled Trigger — Runs the Glue ETL job
# ---------------------------------------------------------------------------
# Fires weekly on Mondays at 12:00 UTC.
# Cron format: cron(minutes hours day-of-month month day-of-week year)

resource "aws_glue_trigger" "glue_schedule" {
  name        = "${var.project_name}-etl-schedule"
  description = "Weekly trigger for the incremental ETL Glue job"
  type        = "SCHEDULED"
  schedule    = "cron(0 12 ? * MON *)"

  actions {
    job_name = aws_glue_job.etl.name
  }
}
