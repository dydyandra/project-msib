from google.oauth2 import service_account
from google.cloud import bigquery
import os, datetime, re, pytz, smtplib, math
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

#os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'C:/Users/Inspiron5000/Desktop/BigQuery Slot Usage Detection/future-data-track-1-17e0c856cf63.json'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = ' '
client = bigquery.Client()


email = ' '
password = ' '

server = smtplib.SMTP_SSL("smtp.gmail.com", 465)
server.login(email, password)
people = {}

def getDuration(start, end):

    duration = end - start
    duration_in_s = duration.total_seconds() 

    def hours():
      return math.ceil(duration_in_s / 3600)

    def minutes():
      return math.ceil(duration_in_s / 60)

    def totalDuration():
        h = hours()
        m = minutes()

        return (h, m)

    return totalDuration()


def notify_user(user_email, job_id):
    string = "gdn-commerce.com"
    if (user_email.__contains__(string)):
        html = f"""\
<!doctype html>
<html>

<head>
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
    <title>Simple Transactional Email</title>
</head>

<body
    style="background-color: #f6f6f6; font-family: sans-serif; -webkit-font-smoothing: antialiased; font-size: 14px; line-height: 1.4; margin: 0; padding: 0; -ms-text-size-adjust: 100%; -webkit-text-size-adjust: 100%;">
    <table role="presentation" border="0" cellpadding="0" cellspacing="0" class="body"
        style="border-collapse: separate; mso-table-lspace: 0pt; mso-table-rspace: 0pt; background-color: #f6f6f6; width: 100%;"
        width="100%" bgcolor="#f6f6f6">
        <tr>
            <td style="font-family: sans-serif; font-size: 14px; vertical-align: top;" valign="top">&nbsp;</td>
            <td class="container"
                style="font-family: sans-serif; font-size: 14px; vertical-align: top; display: block; max-width: 580px; padding: 10px; width: 580px; margin: 0 auto;"
                width="580" valign="top">
                <div class="content"
                    style="box-sizing: border-box; display: block; margin: 0 auto; max-width: 580px; padding: 10px;">
                    
                    <!-- START CENTERED WHITE CONTAINER -->
                    <table role="presentation" class="main"
                        style="border-collapse: separate; mso-table-lspace: 0pt; mso-table-rspace: 0pt; background: #ffffff; border-radius: 3px; width: 100%;"
                        width="100%">

                        <!-- START MAIN CONTENT AREA -->
                        <tr>
                            <td class="wrapper"
                                style="font-family: sans-serif; font-size: 14px; vertical-align: top; box-sizing: border-box; padding: 20px;"
                                valign="top">
                                <table role="presentation" border="0" cellpadding="0" cellspacing="0"
                                    style="border-collapse: separate; width: 100%;" width="100%">
                                    <tr>
                                        <td style="font-family: sans-serif; font-size: 14px; vertical-align: top;"
                                            valign="top">
                                            <p
                                                style="font-family: sans-serif; font-size: 14px; font-weight: normal; margin: 0; margin-bottom: 15px;">
                                                Hi!</p>
                                            <p
                                                style="font-family: sans-serif; font-size: 14px; font-weight: normal; margin: 0; margin-bottom: 15px;">
                                                Your query in BigQuery has been running for a long time. </p>
                                            <p
                                                style="font-family: sans-serif; font-size: 14px; font-weight: normal; margin: 0; margin-bottom: 15px;">
                                                Details of the query:<br>
                                                Job_id : {people[job_id]['id']}<br>
                                                Project : {people[job_id]['project']}<br>
                                                Path of project: {people[job_id]['path']}<br>
                                                URL of resource: {people[job_id]['self_link']}<br><br>


                                                Duration of query running:<br>
                                                - in minutes: {people[job_id]['minutes']} minutes<br>
                                                - in hours: {people[job_id]['hours']} hours<br></p>
                                            <p
                                                style="font-family: sans-serif; font-size: 14px; font-weight: normal; margin: 0; margin-bottom: 15px;">
                                                Try checking your query again. .</p>
                                        </td>
                                    </tr>
                                </table>
                            </td>
                        </tr>

                        <!-- END MAIN CONTENT AREA -->
                    </table>
                    <!-- END CENTERED WHITE CONTAINER -->
                </div>
            </td>
            <td style="font-family: sans-serif; font-size: 14px; vertical-align: top;" valign="top">&nbsp;</td>
        </tr>
    </table>
</body>

</html>
"""
        msg = MIMEMultipart()
        msg['From']=email
        msg['To']=user_email
        msg['Subject']=f"BigQuery Job {people[job_id]['id']} Running For A Long Time"

        msg.attach(MIMEText(html, 'html'))
        server.send_message(msg)
        del msg


         
def get_job(location, job_id):
    job = client.get_job(job_id, location=location)
    hours, minutes = getDuration(job.created, datetime.datetime.now(pytz.utc))
    
    if hours > 2:
        people[job_id] = {'id': job.job_id, 'project': job.project, 'created': job.created.isoformat(), 'minutes': minutes, 'hours': hours, 'path': job.path, 'self_link': job.self_link}
        notify_user(job.user_email, job.job_id)
        
        print(f"{job.project}:{job.job_id}")
        print(f"Type: {job.job_type}")
        print(f"State: {job.state}")
        print(f"Created: {job.created.isoformat()}")
        print(f"Reservation Usage: {job.parent_job_id}")
        print(f"Duration of Running: {hours} hours")
        print(f"Running: {job.running()}")
        print(f"User: {job.user_email}")
        print(f"Path: {job.self_link}")
        print("\n")

        
# jobs run by all users in the project.
for job in client.list_jobs(max_results=20,  all_users=True, state_filter="RUNNING"):
    get_job("us", job.job_id)

    
server.quit()