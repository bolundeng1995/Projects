from outlook_email.email_sender import OutlookEmailSender

if __name__ == "__main__":
    # Create an instance of the OutlookEmailSender class
    email_sender = OutlookEmailSender()

    # Configure email details
    to = "example@example.com"
    subject = "Test Email from Python"
    body = "This is a test email sent via Outlook using Python."
    cc = "cc@example.com"
    bcc = "bcc@example.com"
    attachments = ["C:\\path\\to\\your\\file.txt"]  # Optional

    # Send the email
    email_sender.send_email(to, subject, body, cc, bcc, attachments)