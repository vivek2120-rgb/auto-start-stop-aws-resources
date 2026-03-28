import boto3
import os
import smtplib
import time
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart



# Retrieve environment variables
sender_email = os.environ['sender_email']
sender_email_password = os.environ['sender_email_password']
reciever_email_group = os.environ['reciever_email_group'].split(',')
cluster_names = os.environ['cluster_names'].split(',')
auto_scaling_group_name = os.environ['auto_scaling_group_name'].split(',')
queue_url = os.environ['queue_url']
resource_tag_key = os.environ['resource_tag_key']
stop_time = os.environ['stop_time']
start_time = os.environ['start_time']
environment = os.environ['environment']
account_id = os.environ['account_id']

# Constants used in this script
SUCCESS = "Success"
FAILED = "Failed"
RDS_STARTED = "RDS_STARTED"
SERVICES_STARTED = "SERVICES_STARTED"
SERVICES_STOPPED = "SERVICES_STOPPED"
STARTED = "Started"
STOPPED = "Stopped"

# Initialize AWS clients
ec2 = boto3.client('ec2')
rds = boto3.client('rds')
autoscalling = boto3.client('autoscaling')
sqs = boto3.client('sqs')

def get_last_event_from_queue():
    global last_event
    global sqs_response
    # This variable gets initialized as soon as lambda is triggered.
    # We initialize this variable by reading a message from configured queue.
    sqs_response = sqs.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=1,
        WaitTimeSeconds=5
    )
    #Takes the message from SQS and stores it in last_event variable
    if sqs_response:
        last_event = sqs_response['Messages'][0]['Body']   



def stop_instances(instance_ids):
    instance_status = {}
    failed_instances = {}
    for instance_id in instance_ids:
        try:
            # Stop the instance if they are in a running state
            if ec2.describe_instances(InstanceIds=[instance_id])['Reservations'][0]['Instances'][0]['State']['Name'] == 'running':
                ec2.stop_instances(InstanceIds=[instance_id])
                print(f"Stopped instance: {instance_id}")
                instance_status[instance_id] = SUCCESS
        except Exception as e:
            instance_status[instance_id] = FAILED
            failed_instances[instance_id] = str(e)
            print(f"Error stopping instance {instance_id}: {e}")
    return instance_status, failed_instances   


def start_instances(instance_ids):
    instance_status = {}
    failed_instances = {}
    for instance_id in instance_ids:
         try:
            # Start the EC2 instance if they are in stopped state
            if ec2.describe_instances(InstanceIds=[instance_id])['Reservations'][0]['Instances'][0]['State']['Name'] == 'stopped':
                ec2.start_instances(InstanceIds=[instance_id])
                print(f"Started instance: {instance_id}")
                instance_status[instance_id] = SUCCESS
            else:
                print(f"Instance {instance_id} is not in stopped state, skipping start.")
         except Exception as e:
            instance_status[instance_id] = FAILED
            failed_instances[instance_id] = str(e)
            print(f"Error starting instance {instance_id}: {e}")
    return instance_status, failed_instances

def stop_rds(rds_identifier):
    rds_status = {}
    failed_rds_instances = {}
    for identifier in rds_identifier:
        try:     
            response = rds.stop_db_instance(DBInstanceIdentifier=identifier)            
            rds_status[identifier] = SUCCESS
            print(f"Stopped RDS instance: {response['DBInstance']['DBInstanceIdentifier']}")
        except Exception as e:
            rds_status[identifier] = FAILED
            failed_rds_instances[identifier] = str(e)
            print(f"Error stopping RDS instance {identifier}: {e}")     

    return rds_status, failed_rds_instances  
def start_rds(rds_identifier):
    rds = boto3.client('rds')
    rds_status = {}
    failed_rds_instances = {}
    sucess_rds_instances = []
    for identifier in rds_identifier:
        try:
            response = rds.start_db_instance(DBInstanceIdentifier=identifier)
            rds_status[identifier] = SUCCESS
            sucess_rds_instances.append(identifier)
            print(f"Started RDS instance: {response['DBInstance']['DBInstanceIdentifier']}")
        except Exception as e:
            rds_status[identifier] = FAILED
            failed_rds_instances[identifier] = str(e)
            print(f"Error starting RDS instance {identifier}: {e}")




    return failed_rds_instances

def stop_autoscaling_groups(asg_names):
    for asg_name in asg_names:
        try:
            # Update the desired capacity and min capacity to 0 to stop the ASG    
            response = autoscalling.update_auto_scaling_group(
                AutoScalingGroupName=asg_name,
                DesiredCapacity=0,
                MinSize=0
            )
            print(f"Stopped Auto Scaling Group: {asg_name}")

        except Exception as e:
            print(f"Error stopping Auto Scaling group {asg_name}: {e}")
    time.sleep(2)  # Wait for the ASG to scale down
# Function to check if EC2 instances are part of an Auto Scaling group
# The function expects a list of instance IDs and returns a dictionary with instance IDs as keys and YES/NO as values
def is_ec2_in_asg(instance_ids):
    is_autoscalling_associated = {}
    for instance_id in instance_ids:
        # Check if the instance is part of an Auto Scaling group
        try:
            response = autoscalling.describe_auto_scaling_instances(InstanceIds=[instance_id])
            if response['AutoScalingInstances']:
                is_autoscalling_associated[instance_id] = 'YES'
                print(f"Instance {instance_id} is part of an Auto Scaling group.")
            else:
                print(f"Instance {instance_id} is not part of any Auto Scaling group.")
                is_autoscalling_associated[instance_id] = 'NO'
        except Exception as e:
            print(f"Error checking Auto Scaling group for instance {instance_id}: {e}")
    return is_autoscalling_associated

# Function to retrieve instance names based on their IDs
# The function expects a list of instance IDs and returns a dictionary with instance IDs as keys and
def get_instance_names(instance_ids):
    instance_names = {}
    for instance_id in instance_ids:
        try:
            response = ec2.describe_instances(InstanceIds=[instance_id])
            tags = response['Reservations'][0]['Instances'][0]['Tags']
            for tag in tags:
                if tag['Key'] == 'Name':
                    instance_names[instance_id] = tag['Value']
                    break
        except Exception as e:
            print(f"Error retrieving name for instance {instance_id}: {e}")
    return instance_names

def update_ecs_services_to_zero(cluster_name):
    ecs = boto3.client('ecs')
    services = ecs.list_services(cluster=cluster_name)['serviceArns']
    for service in services:
        try:
            response = ecs.update_service(
                cluster=cluster_name,
                service=service,
                desiredCount=0 # Set desired count to 0 to stop the service
            )
            print(f"Stopped ECS service {service} in cluster {cluster_name}.")
        except Exception as e:
            print(f"Error stopping ECS service {service} in cluster {cluster_name} : {e}")

def update_ecs_services_to_one(cluster_name):
    ecs = boto3.client('ecs')
    services = ecs.list_services(cluster=cluster_name)['serviceArns']
    for service in services:
        try:
            response = ecs.update_service(
                cluster=cluster_name,
                service=service,
                desiredCount=1  # Set desired count to 1
            )
            print(f"Updated ECS service {service} in cluster {cluster_name}.")
        except Exception as e:
            print(f"Error updating ECS service {service} in cluster {cluster_name}: {e}")

def start_auto_scaling_group(asg_names):

        for asg_name in asg_names:
            try:
            # Update the desired capacity and min capacity to 1 to stop the ASG    
                response = autoscalling.update_auto_scaling_group(
                    AutoScalingGroupName=asg_name,
                    DesiredCapacity=1,  # Set desired capacity to 1
                    MinSize=1,          # Set minimum size to 1
                )
                print(f"Started Auto Scaling Group: {asg_name}")                
            except Exception as e:
                print(f"Error updating Auto Scaling Group {asg_name}: {e}")
        time.sleep(10)  # Wait for the ASG to scale up


# Function to send email notifications
# The function expects a list of recipient emails, subject, and body of the email
def send_email(body, account_id, action, failures):
    subject = f"{environment} -{account_id}-Services {action}"
    if failures:
        subject += "(Few Instances Failed)"
    try:
        email = smtplib.SMTP('smtp.outlook.com', 587)
        email.starttls()
        email.login(sender_email, sender_email_password)
        message = f"Subject: {subject}\n\n{body}"
        # Send email to each recipient in the group
        for reciever_email in reciever_email_group:
            msg = MIMEMultipart("alternative")
            msg['From'] = sender_email
            msg['To'] = reciever_email
            msg['Subject'] = subject
            msg.attach(MIMEText(body, 'html'))
            message = msg.as_string()
            email.sendmail(sender_email, reciever_email, message)
            print(f"Email sent to {reciever_email} with subject: {subject}")
        email.quit() # Close the SMTP connection after sending all emails
    except Exception as e:
        print(f"Error sending email: {e}")

def check_rds_status(identifiers):


    rds_status = {}
    overall_rds_status = 'available'
    for identifier in identifiers:
        status = rds.describe_db_instances(DBInstanceIdentifier=identifier)['DBInstances'][0]['DBInstanceStatus']
        if status == 'available':
            rds_status[identifier] = SUCCESS
            print(f"The RDS {identifier} is available")
        else:
            rds_status[identifier] = FAILED
            overall_rds_status = 'not available'
    return overall_rds_status,rds_status

def email_message_template(action,ec2_rows, rds_rows):
    email_message = f"""
        <html>
        <head>
            <style>
                table {{
                    border-collapse: collapse;
                    width: 100%;
                }}
                th, td {{
                    border: 1px solid #dddddd;
                    text-align: left;
                    padding: 8px;
                }}
                th {{
                    background-color: #f2f2f2;
                }}
            </style>
        </head>
        <body>
            <ul>
            <li><strong>This is an auto-generated email. Please do not reply.</strong></li>
            <li><strong>Services are stopped at {stop_time} and started at {start_time}.</strong></li>
            </ul>
            <h2>{action} Resources</h2>
            <table>
                <tr>
                    <th>Instance ID</th>   
                    <th>Instance Type</th>
                    <th>Status</th>
                    <th>Auto Scaling Group</th>
                </tr>
                {ec2_rows}
                {rds_rows}
                
            </table>
        </body>
        </html>
        """
    return email_message
            
# This function stops the EC2 and RDS instances with the tag 'resource_tag_key' set to 'true'
# First it will stop RDS instances followed by EC2 instances
# After stopping, it sends an email notification with the status of each instance 
def stop_applications():
    
    current_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    ec2 = boto3.client('ec2')
    rds = boto3.client('rds')

    try:
        # Retrieve instances with the tag 'resource_tag_key' set to 'true'
        # and store their IDs in a list
        instances = ec2.describe_instances(
            Filters=[
                {
                    'Name': f'tag:{resource_tag_key}',
                    'Values': ['true']
                }
            ]
        )
        # Extract instance IDs from the response
        instance_ids = [instance['InstanceId'] for reservation in instances['Reservations'] for instance in reservation['Instances']]
        
        # Retrieve RDS instances 
        rds_instances_response = rds.describe_db_instances()

        # Initialize an empty list to store RDS instances
        rds_instances = []

        # Extract RDS instance identifiers based on the tag 'resource_tag_key' # and store them in a list
        for db_instance in rds_instances_response['DBInstances']:
            tags_response = rds.list_tags_for_resource(ResourceName=db_instance['DBInstanceArn'])
            tags = {tag['Key']: tag['Value'] for tag in tags_response['TagList']}
            if tags.get(resource_tag_key) == 'true':
                rds_instances.append(db_instance)

        #initialize an empty list to store RDS instance endpoints
        rds_identifier = []
        # Retriving only RDS instance endpoints from total RDS instances data and stores it in rds_identifier list.
        for rds_instance in rds_instances:
            # Check if there are any RDS identifiers that are in started state
            if rds_instance['DBInstanceStatus'] == 'available':
                rds_identifier.append(rds_instance['DBInstanceIdentifier'])
        # initialize an empty dictionary to store RDS instance status        
        rds_status = {}
        failed_rds_instances = {}
        # If there are rds instances in available state, stop the RDS instances
        if rds_identifier:
            rds_status, failed_rds_instances = stop_rds(rds_identifier)
        else:
            print(f"No RDS instances found with the tag {resource_tag_key} set to 'true'.")
        # initialize empty dictionaries to store instance status and failed instances
        instance_status = {}
        failed_instances = {}
        is_autoscalling_associated = {}
        instance_names = {}
        # Stop the auto scaling group if it exists
        if auto_scaling_group_name != ['false']:
            stop_autoscaling_groups(auto_scaling_group_name)
        # If there are instance IDs. 
        if instance_ids:
            instance_names = get_instance_names(instance_ids)
            is_autoscalling_associated = is_ec2_in_asg(instance_ids)

            if cluster_names != ['false']:
                for cluster_name in cluster_names:
                    update_ecs_services_to_zero(cluster_name)

            # Stop the instances
            instance_status, failed_instances = stop_instances(instance_ids)

        else:
            print(f"No EC2 instances found with the tag {resource_tag_key} set to 'true'.")

        # Build EC2 and RDS rows for the HTML table
        ec2_rows = ""
        if instance_status:
            ec2_rows = ''.join(

                f'<tr><td>{instance_names[instance_id]}</td><td>EC2</td><td>{instance_status[instance_id]}</td><td>{is_autoscalling_associated[instance_id]}</td> </tr>'
                for instance_id in instance_status
            )
        rds_rows = ""
        if rds_status:
            rds_rows = ''.join(
                f'<tr><td>{rds_identifier}</td><td>RDS</td><td>{rds_status[rds_identifier]}</td><td>N/A</td></tr>'
                for rds_identifier in rds_status
            )

            

    except Exception as e:
        print(f"An error occurred while stopping EC2 or RDS instances: {e}")


    email_message = email_message_template(STOPPED,ec2_rows, rds_rows)
    send_email(email_message, account_id, STOPPED, failed_instances | failed_rds_instances)
    update_event_in_queue(SERVICES_STOPPED)

        
def update_event_in_queue(new_event):
    try:
        # Let us delete the processed event from the queue, SERVICES_STARTED
        sqs.delete_message(
            QueueUrl=queue_url,
            ReceiptHandle=sqs_response['Messages'][0]['ReceiptHandle']
        )
        print("Deleted the processed message from the queue")


        # Let us push the new event to queue so that next invocation will process the event and take action accordingly 
        sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=new_event,
            MessageGroupId='StartStopResources',  # Required for FIFO queues; groups related messages
            MessageDeduplicationId=f'StartStopResourcesDublication-{int(time.time())}'  # Unique ID to avoid duplication
        )
        print(f"Pushed the new event {new_event} to the queue")        
    except Exception as e:
        print(f"An error occurred while updating the SQS queue: {e}") 

# This function starts the EC2 and RDS instances with the tag 'resource_tag_key' set to 'true'
# First it will start RDS instances 
def start_applications():

    # Retrieve instances with the tag 'resource_tag_key' set to 'True'
    # and store their IDs in a list
    instances = ec2.describe_instances(
        Filters=[
            {
                'Name': f'tag:{resource_tag_key}',
                'Values': ['true']
            }
        ]
    )
    # Extract instance IDs from the response
    instance_ids = [instance['InstanceId'] for reservation in instances['Reservations'] for instance in reservation['Instances']]
    
    # Retrieve RDS instances
    rds_instances_response = rds.describe_db_instances()

    # Initialize an empty list to store RDS instances
    rds_instances = []

    # Extract RDS instance identifiers based on the tag 'resource_tag_key' # and store them in a list
    for db_instance in rds_instances_response['DBInstances']:
        tags_response = rds.list_tags_for_resource(ResourceName=db_instance['DBInstanceArn'])
        tags = {tag['Key']: tag['Value'] for tag in tags_response['TagList']}
        if tags.get(resource_tag_key) == 'true':
            rds_instances.append(db_instance)

    #initialize an empty list to store RDS instance endpoints
    rds_identifier = []
    # Retriving only RDS instance identifiers from total RDS instances data and stores it in rds_identifier list.
    for rds_instance in rds_instances:
        # Check if there are any RDS identifiers that are in stopped state
        rds_identifier.append(rds_instance['DBInstanceIdentifier'])

    # initialize an empty dictionary to store RDS instance status

    failed_rds_instances = {}

    if last_event == SERVICES_STOPPED:
        # If there are rds instances, start the RDS instances
        if rds_identifier:
            failed_rds_instances = start_rds(rds_identifier)    
            if len(failed_rds_instances) > 0:
                return
        else:
            print(f"No RDS instances found with the tag {resource_tag_key} set to 'true'.")

        update_event_in_queue(RDS_STARTED)

    
    if last_event == RDS_STARTED:

        overall_rds_status,rds_status = check_rds_status(rds_identifier)
        if instance_ids:
            instance_names = get_instance_names(instance_ids)
            is_autoscalling_associated = is_ec2_in_asg(instance_ids)
            
        else:
            print(f"No EC2 instances found with the tag {resource_tag_key} set to 'true'.")
        if overall_rds_status == 'available':
            # initialize an empty dictionary to store instance status
            instance_status = {}
            failed_instances = {}
            is_autoscalling_associated = {}
            instance_names = {}    
                # start the auto scaling group if it exists
            if auto_scaling_group_name != ['false']:
                start_auto_scaling_group(auto_scaling_group_name)    
            # Check if there are any ECS clusters to update        
            if cluster_names != ['false']:
                for cluster_name in cluster_names:
                    update_ecs_services_to_one(cluster_name)
            # If there are instance IDs, start the instances
            if instance_ids:
                instance_names = get_instance_names(instance_ids)
                is_autoscalling_associated = is_ec2_in_asg(instance_ids)
                instance_status, failed_instances = start_instances(instance_ids)
            else:
                print(f"No EC2 instances found with the tag {resource_tag_key} set to 'true'.")

            # Build EC2 and RDS rows for the HTML table
            ec2_rows = ""
            if instance_status:
                ec2_rows = ''.join(
                    f'<tr> <td>{instance_names[instance_id]}</td><td>EC2</td><td>{instance_status[instance_id]}</td><td>{is_autoscalling_associated[instance_id]}</td></tr>' 
                    for instance_id in instance_status
                )
            rds_rows = ""
            if rds_status:
                rds_rows = ''.join(
                    f'<tr>   <td>{rds_identifier}</td>   <td>RDS</td>  <td>{rds_status[rds_identifier]}</td> <td>N/A</td>   </tr>' 
                    for rds_identifier in rds_status
                )

        else:
            print("Some RDS are still not available so the instances are not started.")   
            rds_rows = ""
            if rds_status:
                rds_rows = ''.join(
                    f'<tr>   <td>{rds_identifier}</td>   <td>RDS</td>  <td>{rds_status[rds_identifier]}</td> <td>N/A</td>   </tr>' 
                    for rds_identifier in rds_status
                )
            ec2_rows = ""
            if instance_ids:
                ec2_rows = ''.join(
                    f'<tr> <td>{instance_names[instance_id]}</td><td>EC2</td><td> Start not triggered </td><td>{is_autoscalling_associated[instance_id]}</td></tr>' 
                    for instance_id in instance_ids
                )

        email_message = email_message_template(STARTED,ec2_rows, rds_rows)
        send_email(email_message, account_id, STARTED, failed_instances | failed_rds_instances)        

        if overall_rds_status == 'available':
            update_event_in_queue(SERVICES_STARTED)


def lambda_handler(Event, Context): 
    
    get_last_event_from_queue()
    
    print(f"Lambda function has started and event received is: {last_event}")


    if last_event == SERVICES_STARTED:
        stop_applications()
    else:
        start_applications()