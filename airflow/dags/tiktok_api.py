from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
import csv
import boto3

def fetch_data_from_api(**kwargs):
    url = "https://tiktok-api23.p.rapidapi.com/api/user/followers"
    querystring = {"secUid":"MS4wLjABAAAAqB08cUbXaDWqbD6MCga2RbGTuhfO2EsHayBYx08NDrN7IE3jQuRDNNN6YwyfH6_6","count":"30","minCursor":"0","maxCursor":"0"}
    headers = {
        "x-rapidapi-key": "your_api_key",
        "x-rapidapi-host": "tiktok-api23.p.rapidapi.com"
    }

    response = requests.get(url, headers=headers, params=querystring)

    if response.status_code == 200:
        data = response.json()
        user_list = data.get("userList", [])
        extracted_users = []

        for user_info in user_list:
            user = user_info.get("user", {})
            stats = user_info.get("stats", {})

            user_data = {
                "Tiktok_id": user.get("id"),
                "uniqueId": user.get("uniqueId"),
                "nickname": user.get("nickname"),
                "avatarLarger": user.get("avatarLarger"),
                "avatarMedium": user.get("avatarMedium"),
                "avatarThumb": user.get("avatarThumb"),
                "Account_verified": user.get("verified"),
                "privateAccount": user.get("privateAccount"),
                "followers_Count": stats.get("followerCount"),
                "following_Count": stats.get("followingCount"),
                "videoCount": stats.get("videoCount"),
                "heartCount": stats.get("heartCount"),
                "friends_Count": stats.get("friendCount"),
                "diggCount": stats.get("diggCount"),
            }
            
            extracted_users.append(user_data)

        # Save data to XCom
        kwargs['ti'].xcom_push(key='extracted_users', value=extracted_users)

    else:
        raise Exception(f"Failed to fetch data. Status Code: {response.status_code}, Response: {response.text}")


def convert_to_csv(**kwargs):
    extracted_users = kwargs['ti'].xcom_pull(key='extracted_users', task_ids='fetch_data_from_api')

    # Define CSV file path
    csv_file_path = "/path_to_save/tiktok_user_data.csv"

    with open(csv_file_path, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = extracted_users[0].keys()
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        writer.writeheader()
        for user in extracted_users:
            writer.writerow(user)
    
    # Push the file path to XCom
    kwargs['ti'].xcom_push(key='csv_file_path', value=csv_file_path)

def upload_to_s3(**kwargs):
    # Retrieve CSV file path from XCom
    csv_file_path = kwargs['ti'].xcom_pull(key='csv_file_path', task_ids='convert_to_csv')

    s3 = boto3.client('s3',
                      aws_access_key_id='aws_access_key_id',
                      aws_secret_access_key='aws_secret_access_key',
                      region_name='aws_region')

    s3.upload_file(csv_file_path, 'your_bucket_name', 'path_to_upload_file/tiktok_user_data.csv')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 27),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    '',
    default_args=default_args,
    description='A simple data pipeline to fetch TikTok user data and upload it to S3',
    schedule_interval=timedelta(days=1),
)

t1 = PythonOperator(
    task_id='fetch_tiktok_data_from_api',
    python_callable=fetch_data_from_api,
    provide_context=True,
    dag=dag,
)

t2 = PythonOperator(
    task_id='convert_to_csv',
    python_callable=convert_to_csv,
    provide_context=True,
    dag=dag,
)

t3 = PythonOperator(
    task_id='upload_to_s3',
    python_callable=upload_to_s3,
    provide_context=True,
    dag=dag,
)

t1 >> t2 >> t3