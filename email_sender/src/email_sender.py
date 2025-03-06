import win32com.client
from datetime import datetime
from typing import Any
from pathlib import Path

class EmailManager:
    def __init__(self) -> None:
        self.outlook = win32com.client.Dispatch('Outlook.Application')
        self.templates: dict[str, dict[str, Any]] = {}
        
    def register_template(self, template_name: str, subject_template: str, 
                         body_template: str, recipients: list[str]) -> None:
        """Register an email template with recipients"""
        self.templates[template_name] = {
            'subject': subject_template,
            'body': body_template,
            'recipients': recipients
        }
        
    def send_email(self, template_name: str, data: dict[str, Any] | None = None,
                   attachments: list[Path | str] | None = None, 
                   additional_recipients: list[str] | None = None) -> None:
        """Send email using template and dynamic data"""
        if template_name not in self.templates:
            raise KeyError(f"Template '{template_name}' not found")
            
        template = self.templates[template_name]
        mail = self.outlook.CreateItem(0)
        
        # Combine default and additional recipients
        recipients = template['recipients'].copy()
        if additional_recipients:
            recipients.extend(additional_recipients)
        mail.To = "; ".join(recipients)
        
        # Format subject and body with provided data
        format_data = {
            'date': datetime.now().strftime('%Y-%m-%d'),
            **(data or {})
        }
        
        mail.Subject = template['subject'].format(**format_data)
        mail.Body = template['body'].format(**format_data)
        
        # Add attachments
        if attachments:
            for attachment in attachments:
                path = Path(attachment)
                if path.exists():
                    mail.Attachments.Add(str(path))
                    
        mail.Send() 