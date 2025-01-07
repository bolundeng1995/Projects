import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders

class EmailSender:
    def __init__(self, smtp_server, smtp_port, username, password):
        """
        Initialize the SMTP server details.
        :param smtp_server: SMTP server address.
        :param smtp_port: SMTP server port.
        :param username: Email address used for login.
        :param password: Password for the email account.
        """
        self.smtp_server = smtp_server
        self.smtp_port = smtp_port
        self.username = username
        self.password = password

    def send_email(self, recipient, subject, body, cc=None, bcc=None, attachments=None):
        """
        Send an email via SMTP.
        :param recipient: Main recipient(s), comma-separated for multiple.
        :param subject: Email subject.
        :param body: Email body (plain text or HTML).
        :param cc: CC recipient(s), comma-separated for multiple (optional).
        :param bcc: BCC recipient(s), comma-separated for multiple (optional).
        :param attachments: List of file paths to attach (optional).
        """
        try:
            # Create the email
            msg = MIMEMultipart()
            msg['From'] = self.username
            msg['To'] = recipient
            msg['Subject'] = subject
            if cc:
                msg['Cc'] = cc
            msg.attach(MIMEText(body, 'plain'))

            # Add attachments
            if attachments:
                for file in attachments:
                    with open(file, 'rb') as f:
                        part = MIMEBase('application', 'octet-stream')
                        part.set_payload(f.read())
                        encoders.encode_base64(part)
                        part.add_header('Content-Disposition', f'attachment; filename={file}')
                        msg.attach(part)

            # Combine recipients for sending
            all_recipients = recipient.split(",")
            if cc:
                all_recipients += cc.split(",")
            if bcc:
                all_recipients += bcc.split(",")

            # Send the email
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls()
                server.login(self.username, self.password)
                server.sendmail(self.username, all_recipients, msg.as_string())
                print(f"Email sent successfully to {recipient}.")
        except Exception as e:
            raise Exception(f"Failed to send email: {e}")

# Example usage
if __name__ == "__main__":
    # SMTP server configuration
    smtp_server = "smtp.gmail.com"
    smtp_port = 587
    username = "your_email@gmail.com"
    password = "your_password"

    # Create an instance of the EmailSender class
    email_sender = EmailSender(smtp_server, smtp_port, username, password)

    # Configure email details
    to = "recipient@example.com"
    subject = "Test Email from Python (Linux)"
    body = "This is a test email sent via SMTP using Python."
    cc = "cc@example.com"
    bcc = "bcc@example.com"
    attachments = ["/path/to/your/file.txt"]  # Optional

    # Send the email
    email_sender.send_email(to, subject, body, cc, bcc, attachments)