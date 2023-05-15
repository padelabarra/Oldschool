import base64
import os
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Define the sender email address and the directory to save the attachments
sender_email = "ivan.lopez@mbi.cl"
download_dir = "C:\\Users\\pdelabarra\\Documents\\Proyectos_Aplicaciones\\6. Gmail"

# Authenticate with the Gmail API using OAuth2 credentials
creds = Credentials.from_authorized_user_file("credentials.json", scopes=["https://www.googleapis.com/auth/gmail.readonly"])
service = build("gmail", "v1", credentials=creds)

try:
    # Get a list of messages from the sender
    response = service.users().messages().list(userId="me", q=f"from:{sender_email}").execute()
    messages = response.get("messages", [])

    # Iterate through each message and download the attachments
    for message in messages:
        # Get the message details
        message_data = service.users().messages().get(userId="me", id=message["id"]).execute()

        # Check if the message has any attachments
        if "parts" in message_data["payload"]:
            for part in message_data["payload"]["parts"]:
                # Check if the part is an attachment
                if part["filename"] and "attachment" in part["headers"][0]["value"]:
                    # Get the attachment details
                    attachment_id = part["body"]["attachmentId"]
                    attachment = service.users().messages().attachments().get(
                        userId="me", messageId=message["id"], id=attachment_id).execute()

                    # Save the attachment to the specified directory
                    file_data = base64.urlsafe_b64decode(attachment["data"].encode("UTF-8"))
                    file_path = os.path.join(download_dir, part["filename"])
                    with open(file_path, "wb") as f:
                        f.write(file_data)

    print("Attachments downloaded successfully!")
except HttpError as error:
    print(f"An error occurred: {error}")
