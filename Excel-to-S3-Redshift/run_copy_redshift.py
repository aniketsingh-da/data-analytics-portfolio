import psycopg2
import traceback

log_path = r"C:\Users\lenovo\Desktop\redshift_load_log.txt"
conn = None  # Initialize connection variable

try:
    conn = psycopg2.connect(
        dbname='dev',
        user='admin',
        password='BMCPKoyyhy194##',
        host='analytics-wg.932446864230.eu-north-1.redshift-serverless.amazonaws.com',
        port='5439'
    )
    with conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE pharma_patient_data;")
            conn.commit()

            copy_command = """
            COPY pharma_patient_data
            FROM 's3://excel-file-uploads-aniket/pharma_patient_hcp_sample_50.csv'
            IAM_ROLE 'arn:aws:iam::932446864230:role/service-role/AmazonRedshift-CommandsAccessRole-20250722T192607'
            FORMAT AS CSV
            IGNOREHEADER 1
            DATEFORMAT 'auto';
            """
            cur.execute(copy_command)
            conn.commit()

    with open(log_path, "a") as f:
        f.write("Data loaded successfully into pharma_patient_data.\n")

except Exception as e:
    with open(log_path, "a") as f:
        f.write("Error during Redshift load operation:\n")
        f.write(str(e) + "\n")
        f.write(traceback.format_exc() + "\n")

finally:
    if conn:
        conn.close()
