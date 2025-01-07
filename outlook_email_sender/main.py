from outlook_email.email_sender import OutlookEmailSender
from datetime import datetime, timedelta

if __name__ == "__main__":
    email_sender = OutlookEmailSender()

    # Email details
    to = "recipient@example.com"
    subject = "Advanced Email Features Test"
    body = "This is a test email with advanced features."
    cc = "cc@example.com"
    bcc = "bcc@example.com"
    attachments = ["C:\\path\\to\\file.txt"]
    delay_time = datetime.now() + timedelta(minutes=10)  # Send after 10 minutes

    # Send the email with advanced features
    email_sender.send_email(
        recipient=to,
        subject=subject,
        body=body,
        cc=cc,
        bcc=bcc,
        attachments=attachments,
        request_read_receipt=True,
        save_as_draft=False,
        delay_delivery=delay_time,
        priority="High",
        use_signature=True,
    )