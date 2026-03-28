The tech stack used here are
• Python
• AWS Lambda
• AWS EventBridge rules(To trigger the AWS lambda at desired time).

After performing the action the program will send the status of the resources in the email, weather they are stopped or started successfully.
The program starts RDS first and after 30 minutes it will start EC2 to avoid database connection errors.
