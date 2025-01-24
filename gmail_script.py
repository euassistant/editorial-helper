import re
import pandas as pd
from simplegmail import Gmail
from simplegmail.query import construct_query


gmail = Gmail()
labels = gmail.list_labels()

def clean_email(email):
    """ Remove extra quotes around email addresses, including Name <email> format """
    # Remove any leading/trailing quotes
    email = email.strip('"')
    
    # Remove the Name <email> format
    email = re.sub(r'^(.*<)(.*)(>)$', r'\2', email)  # Extract just the email address
    
    return email

def get_replies():
    # Get Replies
    query_params = {
        #"after": '2024/12/10',
        "labels": ["review_submitted"]
    }

    reply_messages = gmail.get_messages(query=construct_query(query_params))
    reply_dict = []
    for message in reply_messages:
        message_info = {
            'To': clean_email(message.recipient),  # Clean the recipient email
            'From': clean_email(message.sender),   # Clean the sender email
            'Subject': clean_email(message.subject),
            'Date Completed': message.date
        }

        reply_dict.append(message_info)

    # Return the list to be converted to a DataFrame later
    return reply_dict

def get_sent():
    # Get Sent Emails
    sent_query_messages = {
        #"after": '2024/12/10',
        "sender": ['eumskcc@gmail.com']
    }
    sent_messages = gmail.get_messages(query=construct_query(sent_query_messages))
    sent_dict = []
    for message in sent_messages:
        message_info = {
            'To': clean_email(message.recipient),  # Clean the recipient email
            'From': clean_email(message.sender),   # Clean the sender email
            'Subject': message.subject,
            'Date Invited': message.date
        }

        sent_dict.append(message_info)

    # Return the list to be converted to a DataFrame later
    return sent_dict

def format_gmail_data():
    # Get the data from both functions and create the DataFrames
    df_a = pd.DataFrame(get_sent())
    df_b = pd.DataFrame(get_replies())

    # Check if 'To', 'From', and 'Subject' exist in both dataframes before merging
    if all(col in df_a.columns for col in ['To', 'From', 'Subject']) and all(col in df_b.columns for col in ['To', 'From', 'Subject']):
        # Perform the merge
        merged_df = pd.merge(df_a, df_b, on=['Subject'], how='left')
        # Save the merged dataframe to CSV
        merged_df.to_csv("cleaned_gmail_data.csv", index=False)
        print("Merged data saved to cleaned_gmail_data.csv")
    else:
        print("One or more required columns are missing from the dataframes")

