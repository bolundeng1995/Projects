import win32com.client as win32
from datetime import datetime, timedelta


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

    def send_email(
        self,
        recipient,
        subject,
        body,
        cc=None,
        bcc=None,
        attachments=None,
        request_read_receipt=False,
        save_as_draft=False,
        delay_delivery=None,
        priority="Normal",
        use_signature=True,
    ):
        """
        Send an email via Outlook with advanced features.

        :param recipient: Main recipient(s), comma-separated for multiple.
        :param subject: Email subject.
        :param body: Email body (plain text or HTML).
        :param cc: CC recipient(s), comma-separated for multiple (optional).
        :param bcc: BCC recipient(s), comma-separated for multiple (optional).
        :param attachments: List of file paths to attach (optional).
        :param request_read_receipt: Request read receipt from recipient (optional).
        :param save_as_draft: Save email as a draft instead of sending it (optional).
        :param delay_delivery: Delay delivery to a specific datetime (optional).
        :param priority: Email priority ('Low', 'Normal', 'High') (optional).
        :param use_signature: Append user's Outlook signature to the email (optional).
        """
        try:
            mail = self.outlook.CreateItem(0)  # 0 corresponds to a MailItem
            mail.To = recipient
            mail.Subject = subject
            if use_signature:
                signature = self.get_signature()
                mail.HTMLBody = f"{body}<br><br>{signature}" if signature else body
            else:
                mail.Body = body  # Use HTMLBody if including signatures or formatting
            if cc:
                mail.CC = cc
            if bcc:
                mail.BCC = bcc

            # Attach files
            if attachments:
                for attachment in attachments:
                    mail.Attachments.Add(attachment)

            # Set priority
            priorities = {"Low": 2, "Normal": 1, "High": 0}
            mail.Importance = priorities.get(priority, 1)  # Default is 'Normal'

            # Request read receipt
            if request_read_receipt:
                mail.ReadReceiptRequested = True

            # Delay delivery
            if delay_delivery:
                mail.DeferredDeliveryTime = delay_delivery.strftime("%m/%d/%Y %H:%M:%S")

            # Save as draft or send
            if save_as_draft:
                mail.Save()
                print("Email saved as a draft.")
            else:
                mail.Send()
                print(f"Email sent successfully to {recipient}.")
        except Exception as e:
            self.log_error(e)
            raise Exception(f"Failed to send email: {e}")

    def get_signature(self):
        """
        Retrieve the user's default Outlook email signature.
        :return: The HTML signature as a string or None if not found.
        """
        try:
            inspector = self.outlook.CreateItem(0).GetInspector
            signature = inspector.HTMLEditor.document.body.innerHTML
            return signature
        except Exception:
            return None

    def log_error(self, error):
        """
        Log errors to a file for troubleshooting.
        :param error: The error message to log.
        """
        with open("error_log.txt", "a") as log_file:
            log_file.write(f"{datetime.now()}: {error}\n")