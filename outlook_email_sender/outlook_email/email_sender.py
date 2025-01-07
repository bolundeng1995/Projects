import win32com.client as win32

class OutlookEmailSender:
    def __init__(self):
        """
        Initialize the Outlook application.
        """
        try:
            self.outlook = win32.Dispatch("Outlook.Application")
            self.namespace = self.outlook.GetNamespace("MAPI")
        except Exception as e:
            raise Exception(f"Failed to initialize Outlook: {e}")

    def send_email(self, recipient, subject, body, cc=None, bcc=None, attachments=None):
        """
        Send an email via Outlook.

        :param recipient: Main recipient(s), comma-separated for multiple.
        :param subject: Email subject.
        :param body: Email body (plain text or HTML).
        :param cc: CC recipient(s), comma-separated for multiple (optional).
        :param bcc: BCC recipient(s), comma-separated for multiple (optional).
        :param attachments: List of file paths to attach (optional).
        """
        try:
            mail = self.outlook.CreateItem(0)  # 0 corresponds to a MailItem
            mail.To = recipient
            mail.Subject = subject
            mail.Body = body  # You can use mail.HTMLBody for HTML content
            if cc:
                mail.CC = cc
            if bcc:
                mail.BCC = bcc
            if attachments:
                for attachment in attachments:
                    mail.Attachments.Add(attachment)
            mail.Send()
            print(f"Email sent successfully to {recipient}.")
        except Exception as e:
            raise Exception(f"Failed to send email: {e}")