import os
import config
from google.cloud import bigquery


def get_credentials():
    credentials_file = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

    if credentials_file:
        return f"Using credentials file: {credentials_file}"
    else:
        return "GOOGLE_APPLICATION_CREDENTIALS environment variable not set."


def main():
    get_credentials()
    # Create a BigQuery client
    client = bigquery.Client()

    # Select project and dataset to retrieve data
    project = 'bigquery-public-data'
    d_set = 'hacker_news'
    dataset_ref = f"{project}.{d_set}"

    """ 
    # Uncomment this section to review how many tables the dataset has
    dataset = client.get_dataset(dataset_ref)
    tables = list(client.list_tables(dataset))
    for table in tables:
        print(table.table_id)
    """

    # Select table to retrieve data
    table_ref = 'full'
    # Construct a reference to the "hacker_news" dataset and table
    table_reference = f"{dataset_ref}.{table_ref}"

    # API request - fetch the table
    table = client.get_table(table_reference)

    client.list_rows(table, max_results=5).to_dataframe()

    # Query to select prolific commenters and post counts
    prolific_commenters_query = """
            SELECT parent, COUNT(1) AS NumPosts
            FROM `bigquery-public-data.hacker_news.full`
            GROUP BY parent
            HAVING COUNT(1) > 10000
    """

    # Set up the query (cancel the query if it would use too much of
    # your quota, with the limit set to 1 GB)
    safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=10 ** 10)
    query_job = client.query(prolific_commenters_query, job_config=safe_config)

    # API request - run the query, and return a pandas DataFrame
    prolific_commenters = query_job.to_dataframe()

    print(prolific_commenters.head())
    # View top few rows of results
    return prolific_commenters.head()


if __name__ == "__main__":
    main()
