"""
Email sending functionality for the task scheduler.

This module provides email functionality using Outlook integration.
"""

import win32com.client as win32
import os
from pathlib import Path
from datetime import datetime
import logging
from typing import Dict, List, Any, Optional, Union
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email.utils import formatdate
from email import encoders
import smtplib

class EmailManager:
    """
    Manages email templates and sending functionality.
    
    This class provides methods for:
    - Registering email templates with placeholders
    - Sending emails based on registered templates
    - Attaching files to emails
    """
    
    def __init__(self):
        self.templates = {}
        self.logger = self._setup_logger()
        
    def _setup_logger(self):
        """Set up a logger for the email manager."""
        logger = logging.getLogger('email_manager')
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            # Create file handler
            log_file = Path('scheduler.log')
            file_handler = logging.FileHandler(log_file)
            file_handler.setLevel(logging.INFO)
            
            # Create formatter and add to handler
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            file_handler.setFormatter(formatter)
            
            # Add handler to logger
            logger.addHandler(file_handler)
            
        return logger
        
    def register_template(self, 
                         template_name: str, 
                         subject: str, 
                         body: str, 
                         recipients: List[str],
                         is_html: bool = False):
        """
        Register an email template for later use.
        
        Args:
            template_name: Unique identifier for the template
            subject: Email subject line (can include {placeholders})
            body: Email body text (can include {placeholders})
            recipients: List of email addresses to send to
            is_html: Whether the email body is HTML content
        """
        self.templates[template_name] = {
            'subject': subject,
            'body': body,
            'recipients': recipients,
            'is_html': is_html
        }
        self.logger.info(f"Registered email template: {template_name}")
        
    def send_email(self, 
                  template_name: str, 
                  data: Dict[str, Any] = None, 
                  attachments: List[Union[str, Path]] = None,
                  additional_recipients: List[str] = None):
        """
        Send an email using a registered template.
        
        Args:
            template_name: Name of the registered template to use
            data: Dictionary of values to substitute in the template
            attachments: List of file paths to attach
            additional_recipients: Additional recipients to add to the template recipients
            
        Returns:
            bool: True if email was sent successfully, False otherwise
        """
        if template_name not in self.templates:
            self.logger.error(f"Email template not found: {template_name}")
            return False
            
        template = self.templates[template_name]
        data = data or {}
        attachments = attachments or []
        
        try:
            # Format subject and body with provided data
            subject = template['subject'].format(**data)
            body = template['body'].format(**data)
            
            # Combine template and additional recipients
            recipients = template['recipients']
            if additional_recipients:
                recipients = recipients + additional_recipients
                
            # Try to use Outlook if available, otherwise use SMTP
            try:
                self._send_via_outlook(subject, body, recipients, attachments, template.get('is_html', False))
            except Exception as e:
                self.logger.warning(f"Failed to send via Outlook: {str(e)}, trying SMTP fallback")
                self._send_via_smtp(subject, body, recipients, attachments, template.get('is_html', False))
                
            self.logger.info(f"Email sent successfully: {subject}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to send email: {str(e)}")
            return False
    
    def _send_via_outlook(self, subject, body, recipients, attachments, is_html=False):
        """Send email using Outlook COM interface."""
        outlook = win32.Dispatch('outlook.application')
        mail = outlook.CreateItem(0)  # 0 = Mail item
        
        mail.Subject = subject
        mail.Body = "" if is_html else body  # If HTML, set body to empty and use HTMLBody
        
        if is_html:
            mail.HTMLBody = body
            
        # Add recipients
        for recipient in recipients:
            mail.Recipients.Add(recipient)
            
        # Add attachments
        for file_path in attachments:
            path = str(file_path) if isinstance(file_path, Path) else file_path
            if os.path.exists(path):
                mail.Attachments.Add(path)
                
        mail.Send()
        
    def _send_via_smtp(self, subject, body, recipients, attachments, is_html=False):
        """Send email using SMTP as a fallback."""
        msg = MIMEMultipart()
        msg['From'] = "scheduler@company.com"  # Replace with actual email address
        msg['To'] = ", ".join(recipients)
        msg['Date'] = formatdate(localtime=True)
        msg['Subject'] = subject
        
        # Attach the email body with appropriate content type
        content_type = "html" if is_html else "plain"
        msg.attach(MIMEText(body, content_type))
        
        # Add attachments
        for file_path in attachments:
            path = str(file_path) if isinstance(file_path, Path) else file_path
            if os.path.exists(path):
                part = MIMEBase('application', "octet-stream")
                with open(path, 'rb') as file:
                    part.set_payload(file.read())
                encoders.encode_base64(part)
                part.add_header('Content-Disposition',
                                f'attachment; filename="{os.path.basename(path)}"')
                msg.attach(part)
        
        # Configure SMTP server settings - these would need to be adjusted for your environment
        smtp_server = "smtp.company.com"  # Replace with actual SMTP server
        smtp_port = 587
        smtp_user = "username"  # Replace with actual SMTP username
        smtp_password = "password"  # Replace with actual SMTP password
        
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()
        server.login(smtp_user, smtp_password)
        server.send_message(msg)
        server.quit() 