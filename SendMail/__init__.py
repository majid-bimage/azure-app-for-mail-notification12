import logging
import os
from flask import Flask
from flask_mail import Mail, Message
from azure.functions import HttpRequest, HttpResponse
import requests
import base64
from datetime import datetime
import pytz
from tzlocal import get_localzone
from datetime import datetime
import pyodbc
app = Flask(__name__)

# Configure logging
logging.basicConfig(level=logging.DEBUG)

# Configure Flask app
app.config['SECRET_KEY'] = 'your-secret-key'  # Change this to a secure secret key
app.config['MAIL_SERVER'] = 'smtp.office365.com'
app.config['MAIL_PORT'] = 587
app.config['MAIL_USE_TLS'] = True
app.config['MAIL_USE_SSL'] = False
app.config['MAIL_USERNAME'] = 'acc.support@bimageconsulting.in'
app.config['MAIL_PASSWORD'] = os.environ.get('MAIL_PASSWORD')
app.config['MAIL_DEFAULT_SENDER'] = 'acc.support@bimageconsulting.in'

# Define your database connection parameters
app.config['server'] = "bimageforge.database.windows.net"
app.config['database'] = "bimageforge"
app.config['username'] = "forge"
app.config['password'] = "BimageNow2020"
app.config['driver'] = 'ODBC Driver 17 for SQL Server'


# Initialize Flask-Mail
mail = Mail(app)
@app.route('/')
async def index(projectid, hubid):
    # Call the function
    role_id= None
    users = None
    # hubid = "9a1a9f2f-235e-4dc9-b961-29f202ea15ca"
    # projectid = "bde8bed9-f5d5-48c7-ac2f-8804f7e58a2b"
    token = await get_2legged_token()
    roles = get_project_roles(hubid, projectid, token)
    for x in roles:
        try:
            if x['name'] == "Receive_Emails":
                print(x['name'] +": "+ x['id'])
                role_id = x['id']
                break
        except Exception as e:
            print(str(e))
    if role_id:
        users = get_users(projectid, role_id, token)


    return users

async def get_2legged_token():
    try:
        client_id = os.environ.get('FORGE_CLIENT_ID')
        client_secret = os.environ.get('FORGE_CLIENT_SECRET')
        combined_string = f"{client_id}:{client_secret}"

        base64_encoded = base64.b64encode(combined_string.encode('utf-8')).decode('utf-8')

        url = "https://developer.api.autodesk.com/authentication/v2/token"


        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Authorization': f'Basic {base64_encoded}'
        }

        data = {
            'grant_type': 'client_credentials',
            'scope': 'account:read data:read'
        }

        response = requests.post(url, headers=headers, data=data)

        if response.status_code == 200:
            response_json = response.json()
            access_token = response_json['access_token']
            return access_token
        else:
            print("Error:", response.status_code)
            return None

    except Exception as ex:
        print("Exception:", str(ex))
        return None

def get_project_roles(hubid, projectid, token):

    url = 'https://developer.api.autodesk.com/hq/v2/accounts/'+hubid+'/projects/'+projectid+'/industry_roles'
    headers = {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer '+token
    }

    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        return f"Error: {response.status_code}, {response.text}"


def get_users(projectId,role_id,token):
    # url = 'https://developer.api.autodesk.com/bim360/admin/v1/projects/'+projectId+'/users'
    url = f'https://developer.api.autodesk.com/construction/admin/v1/projects/{projectId}/users'
    headers = {
        'Authorization': 'Bearer '+token
    }
    
    params = {
        'filter[roleId]' : role_id,
        'limit': 200
    }
    thislist = []
    response = requests.get(url, headers=headers ,params=params)
    if response.status_code == 200:
        results= response.json()
        for x in results['results'] :
            thislist.append(x['email'])
        return thislist
    else:
        return f"Error: {response.status_code}, {response.text}"

async def check_for_gfc(ancestors):
    # Initialize the flag to False
    gfc_found = False
    
    # Iterate through each ancestor in the list
    for ancestor in ancestors:
        # Check if 'GFC' is in the name of the current ancestor
        if 'GFC' in ancestor['name']:
            # Set the flag to True if 'GFC' is found
            gfc_found = True
            break  # Exit the loop since we found 'GFC'
    
    return gfc_found


async def get_project_info(hub_id, project_id):
    print("get_project_info fn begin")
    url = f'https://developer.api.autodesk.com/project/v1/hubs/b.{str(hub_id)}/projects/b.{str(project_id)}'
    bearer_token = await get_2legged_token()

    headers = {
        'Authorization': 'Bearer '+bearer_token
    }
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raise an HTTPError for bad responses

        # Assuming the response is in JSON format
        project_info = response.json()

        project_name = project_info['data']['attributes']['name']
        print(project_name)
        return project_name
    except requests.exceptions.RequestException as e:
        print(f"Error during API request: {e}")
        return str(e)
    
async def get_webview_link(project_id, item_id):
    url= f'https://developer.api.autodesk.com/data/v1/projects/b.{project_id}/items/{item_id}'
    bearer_token = await get_2legged_token()
    headers = {
        'Authorization': 'Bearer '+bearer_token
    }
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raise an HTTPError for bad responses
        item_info = response.json()
        weblink = item_info['data']['links']['webView']['href']
        logging.info(weblink)
        return weblink
    except requests.exceptions.RequestException as e:
        print(f"Error during API request: {e}")
        logging.error(e)
        return str(e)
async def convert_utc_to_local(utc_timestamp, target_timezone):
    # Parse the UTC timestamp string to a datetime object
    utc_datetime = datetime.strptime(utc_timestamp, '%Y-%m-%dT%H:%M:%S%z')

    # Define the target timezone
    target_tz = pytz.timezone(str(target_timezone).upper())

    # Convert UTC datetime to the target timezone
    local_datetime = utc_datetime.astimezone(target_tz)

    # Format the result as a string
    local_timestamp = local_datetime.strftime('%Y-%m-%dT%H:%M:%S%z')
    timestamp_modified = local_timestamp.replace('T', ' ').replace('+', ' +')
    return timestamp_modified
async def create_schema_if_not_exists(schema_name):
    try:
        # Create a connection string
        app.config['conn_str'] = f"DRIVER={{{app.config['driver']}}};SERVER={app.config['server']};DATABASE={app.config['database']};UID={app.config['username']};PWD={app.config['password']}"
        # Establish a connection
        conn = pyodbc.connect(app.config['conn_str'])

        # Create a cursor object
        cursor = conn.cursor()
        # Check if the schema exists
        cursor.execute(f"SELECT schema_id FROM sys.schemas WHERE name = '{schema_name}'")
        schema_exists = cursor.fetchone()

        if not schema_exists:
            # Create the schema if it doesn't exist
            cursor.execute(f"CREATE SCHEMA {schema_name}")
            logging.info("Schema created")
            conn.commit()
            return True
        else:
            logging.info("Schema Exists")
            return True
    except Exception as ex:
        logging.info(ex)
        return False
async def create_table_if_not_exists(schema_name, table_name):
    try:
        # Create a connection string
        app.config['conn_str'] = f"DRIVER={{{app.config['driver']}}};SERVER={app.config['server']};DATABASE={app.config['database']};UID={app.config['username']};PWD={app.config['password']}"
        # Establish a connection
        conn = pyodbc.connect(app.config['conn_str'])

        # Create a cursor object
        cursor = conn.cursor()
        # Check if the table exists
        # Assuming schema_name and table_name are variables holding the schema and table names
        query = f"""
            SELECT *
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = '{schema_name}' AND TABLE_NAME = '{table_name}'
        """

        cursor.execute(query)
        table_exists = cursor.fetchone()


        if not table_exists:
            # Create the table if it doesn't exist
            cursor.execute(f"""
                IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{schema_name}.{table_name}')
                    BEGIN
                        CREATE TABLE {schema_name}.{table_name} (
                            id INT IDENTITY(1,1) PRIMARY KEY,
                            hookid VARCHAR(255) NOT NULL,
                            urn VARCHAR(255) NOT NULL,
                            lastmodifiedtime VARCHAR(255),
                            lastmodifieduser VARCHAR(255),
                            filename VARCHAR(255),
                            projectname VARCHAR(255),
                            projectpath VARCHAR(255),
                            lastupdatedtime VARCHAR(255),
                            CONSTRAINT unique_urn UNIQUE (urn)
                        );
                    END;
                    """)
            logging.info("table created")
            conn.commit()
            return True
        else:
            logging.info("Table already exists")
            return True
    except Exception as ex:
        logging.info(ex)
        return False
async def insert_data(schema_name, table_name, hookid, urn, lastmodifiedtime, lastmodifieduser, filename, projectname, projectpath):
    # Create a connection string
    app.config['conn_str'] = f"DRIVER={{{app.config['driver']}}};SERVER={app.config['server']};DATABASE={app.config['database']};UID={app.config['username']};PWD={app.config['password']}"
    # Establish a connection
    conn = pyodbc.connect(app.config['conn_str'])

    # Create a cursor object
    cursor = conn.cursor()
    # Check if a row with the same attributes already exists
    query_check = f"""
        SELECT COUNT(*)
        FROM {schema_name}.{table_name}
        WHERE urn = ? AND lastmodifiedtime = ? AND lastmodifieduser = ? AND filename = ?
    """
    cursor.execute(query_check, (urn, lastmodifiedtime, lastmodifieduser, filename))
    row_count = cursor.fetchone()[0]
    if row_count == 0:
        # Insert the new row
        query = f"""
            INSERT INTO {schema_name}.{table_name} (hookid, urn, lastmodifiedtime, lastmodifieduser, filename, projectname, projectpath, lastupdatedtime)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """

        # Assuming lastupdatedtime is the current datetime
        current_datetime = datetime.now()

        # Execute the insert query with parameters
        cursor.execute(query, (hookid, urn, lastmodifiedtime, lastmodifieduser, filename, projectname, projectpath, current_datetime))
        conn.commit()

        # Close the cursor and connection
        cursor.close()
        conn.close()
        return True
    else:
        logging.info("Row with the same attributes already exists. Not inserting.")
        return False
async def main(req: HttpRequest) -> HttpResponse:
    logging.info("function begin")
    # target_timezone = "Asia/Kolkata"
    target_timezone = get_localzone()
    logging.info(target_timezone)

    recipient = ["avis@bimageconsulting.in","majid.n@bimageconsulting.in"]
    # avis@bimageconsulting.in
    subject = "Bim Folder Notification"
    message_body = "test message body - Hi this is test mail sent on 01/02/2024 13:59"

    try:

        if req.method == 'POST':
            # Create a connection string
            app.config['conn_str'] = f"DRIVER={{{app.config['driver']}}};SERVER={app.config['server']};DATABASE={app.config['database']};UID={app.config['username']};PWD={app.config['password']}"
            # Establish a connection
            conn = pyodbc.connect(app.config['conn_str'])

            # Create a cursor object
            cursor = conn.cursor()
            # Process the data received in the callback
            data = req.get_json()  # Assuming the data is in JSON format
        # data = {'version': '1.0', 'resourceUrn': 'urn:adsk.wipprod:fs.file:vf.ZY4iW_eER-6e8Ee5OWnREQ?version=1', 'hook': {'hookId': '1ae09bae-7fd6-4890-b09d-ac6ebc75f036', 'tenant': 'urn:adsk.wipprod:fs.folder:co.JnqBvg6pTNSCMfhkMr1ezw', 'callbackUrl': 'https://7097-103-214-235-230.ngrok-free.app/send_email', 'createdBy': 'Ak5xhjoOVN80nIGnGXBgWtWf1LS6GbWA', 'event': 'dm.version.added', 'createdDate': '2024-02-01T10:13:56.605+00:00', 'lastUpdatedDate': '2024-02-01T10:13:56.605+00:00', 'system': 'data', 'creatorType': 'Application', 'status': 'active', 'scope': {'folder': 'urn:adsk.wipprod:fs.folder:co.JnqBvg6pTNSCMfhkMr1ezw'}, 'autoReactivateHook': False, 'urn': 'urn:adsk.webhooks:events.hook:1ae09bae-7fd6-4890-b09d-ac6ebc75f036', 'callbackWithEventPayloadOnly': False, '__self__': '/systems/data/events/dm.version.added/hooks/1ae09bae-7fd6-4890-b09d-ac6ebc75f036'}, 'payload': {'ext': 'pdf', 'modifiedTime': '2024-02-01T10:15:03+0000', 'creator': 'F4R27ZLHJ3DMFDD6', 'lineageUrn': 'urn:adsk.wipprod:dm.lineage:ZY4iW_eER-6e8Ee5OWnREQ', 'sizeInBytes': 44263, 'hidden': False, 'indexable': True, 'source': 'urn:adsk.wipprod:fs.file:vf.ZY4iW_eER-6e8Ee5OWnREQ?version=1', 'version': '1', 'user_info': {'id': 'F4R27ZLHJ3DMFDD6'}, 'name': '120. AMS VS AMS.pdf', 'context': {'lineage': {'reserved': False, 'reservedUserName': None, 'reservedUserId': None, 'reservedTime': None, 'unreservedUserName': None, 'unreservedUserId': None, 'unreservedTime': None, 'createUserId': 'F4R27ZLHJ3DMFDD6', 'createTime': '2024-02-01T10:15:03+0000', 'createUserName': 'Majid N', 'lastModifiedUserId': 'F4R27ZLHJ3DMFDD6', 'lastModifiedTime': '2024-02-01T10:15:03+0000', 'lastModifiedUserName': 'Majid N'}, 'operation': 'PostVersionedFiles'}, 'createdTime': '2024-02-01T10:15:03+0000', 'modifiedBy': 'F4R27ZLHJ3DMFDD6', 'state': 'CONTENT_AVAILABLE', 'parentFolderUrn': 'urn:adsk.wipprod:fs.folder:co.kVQof2GfSGKsqf9QMDMbmw', 'ancestors': [{'name': '9a1a9f2f-235e-4dc9-b961-29f202ea15ca-account-root-folder', 'urn': 'urn:adsk.wipprod:fs.folder:co.8DhXKk-fTCuOB7lro19mDw'}, {'name': 'ace3d80e-a6e9-4707-8809-7a9d0b065e45-root-folder', 'urn': 'urn:adsk.wipprod:fs.folder:co.Bo2foW1bRzSQ-5Lu9yrWjw'}, {'name': 'Project Files', 'urn': 'urn:adsk.wipprod:fs.folder:co.JnqBvg6pTNSCMfhkMr1ezw'}, {'name': 'Test03', 'urn': 'urn:adsk.wipprod:fs.folder:co.kVQof2GfSGKsqf9QMDMbmw'}], 'project': 'ace3d80e-a6e9-4707-8809-7a9d0b065e45', 'tenant': '9a1a9f2f-235e-4dc9-b961-29f202ea15ca', 'custom-metadata': {'storm:process-state': 'NEEDS_PROCESSING', 'dm_sys_id': 'e59abf0d-3ba6-4dca-b393-b96e363ddc77', 'file_name': '120. AMS VS AMS.pdf', 'lineageTitle': '', 'dm_command:id': '8cf2e641-7884-415c-9c5f-88835c8decc8', 'forge.type': 'versions:autodesk.bim360:File-1.0', 'storm:entity-type': 'SEED_FILE', 'fileName': '120. AMS VS AMS.pdf'}}}
        # Extracting relevant information
            # logging.info(data)
            creator_username = data['payload']['context']['lineage']['lastModifiedUserName']  # Use 'lastModifiedUserName' for creator
            created_time = data['payload']['createdTime']
            
            project = data['payload']['project']
            hubid = data['payload']['tenant']
            item_id = data['payload']['lineageUrn']
            weblink = await get_webview_link(project, item_id)

            project_name = await get_project_info(hubid, project)
            print(project_name)
            # Creating a string mentioning the path of the folder using ancestors
            ancestors = data['payload']['ancestors']
            gfc_found = await check_for_gfc(ancestors)
            folder_path = '/'.join(folder['name'] for folder in ancestors if 'root-folder' not in folder['name'])
            folder_path = project_name +'/'+ folder_path
            lastModifiedTime = data['payload']['context']['lineage']['lastModifiedTime']
            lastModifiedUserName = data['payload']['context']['lineage']['lastModifiedUserName']
            local_timestamp = await convert_utc_to_local(lastModifiedTime, target_timezone)
            lastModifiedTime = lastModifiedTime.replace('T', ' ').replace('+', ' +')

            # Additional information
            file_name = data['payload']['name']
            file_version = data['payload']['version']
            file_size = data['payload']['sizeInBytes']
            hook_id = data['hook']['hookId']

            # Example: Send an email with the extracted information
            email_body = f"""
            <!DOCTYPE html>
            <html lang="en">
                <head>
                    <meta charset="UTF-8">
                    <meta name="viewport" content="width=device-width, initial-scale=1.0">
                    <style>
                        body {{
                            font-family: Arial, sans-serif;
                            margin: 20px;
                        }}
                        .container {{
                            border: 1px solid #ccc;
                            padding: 20px;
                            max-width: 600px;
                            margin: 0 auto;
                            border-radius: 5px;
                        }}
                        .header {{
                            background-color: #f0f0f0;
                            padding: 10px;
                            text-align: center;
                            border-radius: 5px 5px 0 0;
                        }}
                        .content {{
                            margin-top: 20px;
                        }}
                        .footer {{
                            margin-top: 20px;
                            font-size: 12px;
                            color: #777;
                        }}
                    </style>
                </head>
                <body>
                    <div class="container">
                        <div class="header">
                            <h2>Email Notification</h2>
                        </div>
                        <div class="content">
                            <p>
                                <strong>{file_name}</strong> has been modified/updated by <strong>{creator_username}</strong>
                                                in <strong>{folder_path}</strong>, at  <strong>{local_timestamp} </strong>.
                            </p>
                            <p>
                                    View the details <a href="{weblink}">here</a>   
                            </p>
                            <hr>
                            <p>This email was generated by  <strong>BIMAGE Consulting</strong>.</p>
                        </div>
                        <div class="footer">
                            <p>© 2024 BIMAGE Consulting. All rights reserved.</p>
                        </div>
                    </div>
                </body>
            </html>
            """
            if gfc_found != False:
                existence= False
                table = False
                schema = False
                schema = await create_schema_if_not_exists('hooksmail')
                if schema:
                    table = await create_table_if_not_exists('hooksmail', 'hooksentry')
                if table:
                    existence = await insert_data('hooksmail', 'hooksentry',hook_id, item_id, lastModifiedTime, lastModifiedUserName, file_name, project_name, folder_path)
                if existence:
                    recipient = await index(project, hubid)

                    # return 'Callback received. Nothing to process.'
                    # Create a message object
                    with app.app_context():
                        message = Message(subject=subject, recipients=recipient, html=email_body)
                        # Send the email
                        mail.send(message)
                        # flash('Email sent successfully!', 'success')
                        return HttpResponse("Success")
                else:
                    return HttpResponse("Already passed hook")
            else:
                logging.info("Resource not in GFC Folder")
                return HttpResponse("Resource not in GFC Folder...")
    except Exception as e:
        print(str(e))
        logging.info(str(e))
        return HttpResponse(str(e))
        # flash(f'Error sending email: {str(e)}', 'error')

    # return redirect(url_for('index'))
    
                