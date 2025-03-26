from google.cloud import storage
from google.cloud import documentai_v1
from google.cloud import bigquery
from google.cloud import pubsub_v1
import functions_framework
import json
from datetime import datetime  # Added for timestamp

@functions_framework.cloud_event
def process_ocr(cloud_event):
    data = cloud_event.data
    bucket_name = data["bucket"]
    file_name = data["name"]

    # Extract student_id and assignment_id
    parts = file_name.split('/')
    assignment_id = parts[1]  # e.g., assignments/A1/student123.pdf
    student_id = parts[2].split('.')[0]

    # OCR with Document AI
    client = documentai_v1.DocumentProcessorServiceClient()
    name = "projects/mineral-subject-450718-j1/locations/us/processors/7f90007cdf1df941"  # Replace with your processor ID
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    content = blob.download_as_bytes()

    request = documentai_v1.ProcessRequest(
        name=name,
        raw_document=documentai_v1.RawDocument(content=content, mime_type="application/pdf")
    )
    result = client.process_document(request=request)
    extracted_text = result.document.text

    # Store in BigQuery
    bq_client = bigquery.Client()
    table_id = "mineral-subject-450718-j1.education_data.Student_answers"
    rows = [{
        "student_id": student_id,
        "assignment_id": assignment_id,
        "extracted_text": extracted_text,
        "timestamp": datetime.utcnow().isoformat()  # Use current UTC time
    }]
    errors = bq_client.insert_rows_json(table_id, rows)
    if errors:
        raise Exception(f"BigQuery insert failed: {errors}")

    # Publish to Pub/Sub
    publisher = pubsub_v1.PublisherClient()
    topic_path = "projects/mineral-subject-450718-j1/topics/student-answer-processed"
    message_data = json.dumps({
        "student_id": student_id,
        "assignment_id": assignment_id
    }).encode("utf-8")
    publisher.publish(topic_path, message_data)

    return f"Processed and published {file_name}"