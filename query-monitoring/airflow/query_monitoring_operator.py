import email
from os import remove
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.models import Variable
from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from contrib.operators.ms_team_webhook_operator import MSTeamsWebhookOperator
from airflow.utils.email import send_email
from google.cloud import bigquery
from datetime import datetime
import logging
import math
import pytz
import pendulum

class QueryMonitoringOperator(BaseOperator):
    """
    Operator for monitor queries in BigQuery accross several projects.
    :param list_projects: the list of the projects that will be checked. 
    :type list_projects: list
    :param minute_filter: minute filter of the query (if running time of query > minute_filter then user will be alerted). 
    :type minute_filter: int
    :param substring: the email/user substring who submit the query that will be included. 
    :type substring: str
    :param error_file_email_recipient: list of the email that will be alerted. 
    :type error_file_email_recipient: str
    :param bigquery_conn_id: reference to a specific BigQuery hook.
    :type bigquery_conn_id: str
    :param location: the location of the slots that will be removed, default will be US. 
    :type location: str
    """

    @apply_defaults
    def __init__(self,
                 list_projects,
                 minute_filter,
                 substring,
                 error_file_email_recipient=Variable.get('email_alert_recipient'),
                 bigquery_conn_id='bigquery_default',
                 location='US',
                 *args,
                 **kwargs):
        super(QueryMonitoringOperator, self).__init__(*args, **kwargs)
        self.list_projects = list_projects
        self.minute_filter = minute_filter
        self.substring = substring
        self.error_file_email_recipient = error_file_email_recipient
        self.bigquery_conn_id = bigquery_conn_id
        self.location = location

    def get_duration(self, start, end):
        duration = end - start
        duration_in_s = duration.total_seconds()
        minutes = math.ceil(duration_in_s/60)
        minutes = float("{:.2f}".format(minutes))

        return minutes

    def send_email_alert(self, user_email, job_id, project, created, minutes):
        subject = "BigQuery Query Monitoring Alert"
        body = """
        Hi {0}, <br>
        Your Query with job_id {1} in BigQuery project {2} have been running for quite some time. <br>
        <br>
        Query Details: <br>
        User email: {0} <br>
        Job ID: {1} <br>
        Project: {2} <br>
        Created time: {3} <br>
        Duration running: {4} minutes <br>
        <br>
        Please check if your query is running properly, Thank you. 
        """.format(user_email, job_id, project, created, minutes)

        email_list = [recipient.strip() for recipient in self.error_file_email_recipient.split(',')]
        email_list.append(user_email)

        send_email(email_list, subject, body)

    def send_teams_alert(self, job_id, minutes, project, user_email):
        messages = """Query Monitoring Alert <br>
        job_id: {0} has been running for over {1} minutes in project {2} by user_email {3}. Please do check and notify the user. Thank you
        """.format(job_id, minutes, project, user_email)

        teams_notification = MSTeamsWebhookOperator(
            task_id="msteams_notify_alert",
            trigger_rule="all_done",
            message=messages,
            theme_color="FF0000",
            http_conn_id='msteams_webhook_url'
        )
        teams_notification.execute(messages)

    def execute (self, context):
        tz = pendulum.timezone('Asia/Jakarta')
        bigquery_hook = BigQueryHook(bigquery_conn_id=self.bigquery_conn_id, use_legacy_sql=False)
        credentials = bigquery_hook._get_credentials()
        client = bigquery.Client(credentials=credentials)

        for project in self.list_projects:
            for job in client.list_jobs(state_filter="RUNNING", project=project, all_users=True):
                query_job = client.get_job(job.job_id, project=project, location=self.location)
                minutes = self.get_duration(query_job.created, datetime.now(pytz.utc))
                created = tz.convert(query_job.created)

                if minutes > self.minute_filter and self.substring in query_job.user_email:
                    logging.info(f"Job ID:{query_job.job_id}")
                    logging.info(f"Type: {query_job.job_type}")
                    logging.info(f"State: {query_job.state}")
                    logging.info(f"Created: {created}")
                    logging.info(f"Duration: {minutes} minutes")
                    logging.info(f"User: {query_job.user_email}")

                    self.send_email_alert(query_job.user_email, query_job.job_id, project, 
                                            created, minutes)
                    self.send_teams_alert(query_job.job_id, minutes, project, query_job.user_email)