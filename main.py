import re
import os
import pandas as pd
from simplegmail import Gmail
from datetime import datetime
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



def get_local_data():
    folder_path = '../Dealt With/'
    pattern = r'([A-Za-z]+)-D-(\d{2}-\d{5})(R\d+)\s\((.*?)\)\s(\d{4})-(\d{2})-(\d{2})\.pdf'    
    files = os.listdir(folder_path)
    file_data = []
    for file_name in files:
        file_path = os.path.join(folder_path, file_name)
        if os.path.isfile(file_path):  # Check if it's a file, not a directory
            match = re.search(pattern, file_name)
            if match:
                journal = match.group(1)  # EUONCO
                ms_number = f"{journal}-D-{match.group(2)}{match.group(3)}"  # EUONCO-D-24-00622R2
                version = match.group(3)  # R2
                editor = match.group(4)   # Assel
                year = match.group(5)     # 2024
            
                file_info = {
                    'Name': file_name,
                    'MS Number': ms_number,
                    'Version': version,
                    'Year': year,
                    'Editor': editor,
                    'Journal': journal,
                }
                file_data.append(file_info)
    
    df = pd.DataFrame(file_data)
    df.to_csv('file_list.csv', index=False)
    print("File list created")


def merge_data():
    reviewer_metrics_2024 = pd.read_csv("reviewer-metrics.csv")
    updated_file = pd.read_csv("file_list.csv")

    merged_df = pd.merge(updated_file, reviewer_metrics_2024, on=['Name','MS Number','Version','Year','Editor','Journal'], how='outer')
    # Save the merged dataframe to CSV
    current_date = datetime.now().strftime('%Y-%m-%d')
    filename = f"merged_{current_date}.csv"
    merged_df.to_csv(filename, index=False)
    print("File saved")


def main():
    format_gmail_data()
    #get_local_data()
    #merge_data()

main()
