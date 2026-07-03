import smtplib
import ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os

# Your Gmail credentials (use an App Password, NOT your regular password)
sender_email = "phanibaddireddy@gmail.com"
app_password = "vcyx lwza ilhi siwj"

# Recipient's email address
recipient_email = "rameshk.cloud4c@gmail.com"

# Email content
subject = "Python Notification"
body = "This is a test notification from your Python script!"

# Create a multipart message and set headers
message = MIMEMultipart()
message["From"] = sender_email
message["To"] = recipient_email
message["Subject"] = subject
message.attach(MIMEText(body, "plain"))

# Create a secure SSL context and connect to the SMTP server
context = ssl.create_default_context()
with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
    try:
        # Log in to your Gmail account
        server.login(sender_email, app_password)

        # Send the email
        server.send_message(message)
        print("Email notification sent successfully!")

    except smtplib.SMTPAuthenticationError as e:
        print(f"Authentication failed. Check your email and app password. Error: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")