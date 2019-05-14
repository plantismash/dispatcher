# This file is part of antiSMASH.
#
# antiSMASH is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# antiSMASH is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with antiSMASH.  If not, see <http://www.gnu.org/licenses/>.
"""Send email notifications to users"""
import os
import smtplib
from datetime import datetime

FROMADDR = os.getenv('ANTISMASH_EMAIL_FROM', "antismash@localhost")
ERRORADDR = os.getenv('ANTISMASH_EMAIL_ERROR', FROMADDR)
SMTP_SERVER = os.getenv('ANTISMASH_EMAIL_HOST', "localhost")
SMTP_ENCRYPT = os.getenv('ANTISMASH_EMAIL_ENCRYPT', 'no').lower()
SMTP_USERNAME = os.getenv('ANTISMASH_EMAIL_USER', '')
SMTP_PASSWORD = os.getenv('ANTISMASH_EMAIL_PASSWORD', '')
BASE_URL = os.getenv('ANTISMASH_BASE_URL', 'http://antismash.secondarymetabolites.org')
TOOL_NAME = os.getenv('ANTISMASH_TOOL_NAME', 'antiSMASH')

message_template = """From: %(from)s
To: %(to)s
Subject: Your %(tool)s job %(jobid)s finished.
Date: %(currdate)s

Dear %(tool)s user,

The %(tool)s job '%(jobid)s' you submitted on %(submitdate)s with the
filename '%(filename)s' has finished with status '%(status)s'.

%(action_string)s

If you found %(tool)s useful, please check out
%(base_url)s/about.html
for information on citing %(tool)s.

"""

success_template = """You can find the results on
%(base_url)s/upload/%(jobid)s/%(result_file)s"""

failure_template = """Please contact %(from)s to resolve the issue"""

error_message_template = """From: %(from)s
To: %(from)s
Subject: [%(jobtype)s] job %(jobid)s failed
Date: %(currdate)s

The %(tool)s job %(jobid)r submitted on %(submitdate)s has failed.
Dispatcher: %(dispatcher)s
Input file: %(base_url)s/upload/%(jobid)s/%(filename)s %(gff3_line)s
Log file: %(base_url)s/upload/%(jobid)s/%(jobid)s.log
User email: %(user)s
Status '%(status)s'.
"""


def send_mail(job):
    """Send an email about the given job to a user"""
    if not job.email:
        return
    msg = compose_message(job)
    try:
        handle_send(FROMADDR, job.email, msg)
    except Exception as e:
        print "Failed to send mail: %s" % e


def compose_message(job):
    """Construct the message according to job status"""
    blocks = {"from": FROMADDR,
              "to": job.email,
              "jobid": job.uid,
              "submitdate": job.added,
              "filename": job.filename,
              "tool": TOOL_NAME,
              "base_url": BASE_URL,
              "result_file": "index.html",
              "currdate": datetime.utcnow().strftime("%a, %d %b %Y %H:%M:%S +0000"),
              "status": job.status}

    if job.status.startswith("done"):
        result_string = success_template
    else:
        result_string = failure_template

    blocks['action_string'] = result_string % blocks

    return message_template % blocks


def send_error_mail(job):
    """Send an email about the failed job to the admin"""
    blocks = {"from": FROMADDR,
              "to": ERRORADDR,
              "tool": TOOL_NAME,
              "base_url": BASE_URL,
              "jobid": job.uid,
              "jobtype": job.jobtype,
              "filename": job.filename,
              "gff3": job.gff3,
              "submitdate": job.added,
              "currdate": datetime.utcnow().strftime("%a, %d %b %Y %H:%M:%S +0000"),
              "dispatcher": job.dispatcher,
              "user": job.email,
              "status": job.status}

    gff3_line = ''
    if job.gff3:
        gff3_line = '\n%(base_url)s/upload/%(jobid)s/%(gff3)s' % blocks

    blocks['gff3_line'] = gff3_line

    msg = error_message_template % blocks
    try:
        handle_send(FROMADDR, ERRORADDR, msg)
    except Exception as e:
        print "Failed to send error mail: %s" % e


def handle_send(from_addr, to_addr, message):
    """Handle the actual email sending"""
    if SMTP_ENCRYPT == 'no':
        server = smtplib.SMTP(SMTP_SERVER, 587)
    elif SMTP_ENCRYPT == 'tls':
        server = smtplib.SMTP(SMTP_SERVER, 587)
        server.starttls()
        if SMTP_USERNAME != '' and SMTP_PASSWORD != '':
            server.login(SMTP_USERNAME, SMTP_PASSWORD)
    elif SMTP_ENCRYPT == 'ssl':
        server = smtplib.SMTP_SSL(SMTP_SERVER)
        if SMTP_USERNAME != '' and SMTP_PASSWORD != '':
            server.login(SMTP_USERNAME, SMTP_PASSWORD)
    else:
        raise Exception('Invalid email configuration')
    server.sendmail(from_addr, [to_addr], message)
    server.quit()
