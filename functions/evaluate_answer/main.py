from google.cloud import bigquery
import google.generativeai as genai
from google.generativeai.types import GenerationConfig
import functions_framework
import json
import base64
from datetime import datetime
import os

# Configure the Gemini model (assumes API key is set in env vars)
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] =  "service.json" # Set GOOGLE_API_KEY in Cloud Function env vars
genai.configure(api_key=os.getenv("GOOGLE_API_KEY"))

@functions_framework.cloud_event
def evaluate_answer(cloud_event):
    # Decode Pub/Sub message
    message_data = base64.b64decode(cloud_event.data["message"]["data"]).decode("utf-8")
    data = json.loads(message_data)
    student_id = data["student_id"]
    assignment_id = data["assignment_id"]

    # Fetch extracted text and assignment details
    bq_client = bigquery.Client()
    query = f"""
        SELECT sa.extracted_text, a.question, a.criteria
        FROM `mineral-subject-450718-j1.education_data.Student_answers` sa  # Fixed typo
        JOIN `mineral-subject-450718-j1.education_data.assignments` a
        ON sa.assignment_id = a.assignment_id
        WHERE sa.student_id = '{student_id}' AND sa.assignment_id = '{assignment_id}'
    """
    result = bq_client.query(query).result()
    row = next(result)
    extracted_text, question, criteria = row[0], row[1], row[2]

    # Initialize Gemini model
    model = genai.GenerativeModel(
        model_name="gemini-1.5-pro-002",
        generation_config=GenerationConfig(
            temperature=0.2,
            top_p=0.95,
            max_output_tokens=8192
        )
    )

    # Evaluate with Gemini, explicitly asking for numeric score
    prompt = f"""
        Evaluate the following student answer based on the given criteria and provide a score out of 10 along with detailed feedback.
        Return the score as a plain integer (e.g., 5), not a fraction (e.g., 5/10).

        Question: {question}
        Student Answer: {extracted_text}
        Criteria: {criteria}

        Score: [Provide a score out of 10 as an integer]
        Feedback: [Provide detailed feedback explaining the score]
    """
    response = model.generate_content(prompt)
    response_text = response.text
    score = int(response_text.split("Score:")[1].split("\n")[0].strip())
    feedback = response_text.split("Feedback:")[1].strip()

    # Store in BigQuery
    table_id = "mineral-subject-450718-j1.education_data.evaluations"
    rows = [{
        "student_id": student_id,
        "assignment_id": assignment_id,
        "score": score,
        "feedback": feedback,
        "timestamp": datetime.utcnow().isoformat()
    }]
    bq_client.insert_rows_json(table_id, rows)

    return f"Evaluated {student_id} for {assignment_id}"
