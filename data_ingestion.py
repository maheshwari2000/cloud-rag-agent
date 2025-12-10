import boto3
import json
import uuid
from botocore.exceptions import ClientError

# CONFIG
REGION = "us-east-2"
RAW_BUCKET = "kaggle-arxiv-dataset"
RAW_KEY = "arxiv-metadata-oai-snapshot.json"
VECTOR_BUCKET = "kaggle-arxiv-vector-bucket"
INDEX_NAME = "arxiv-index"
DYNAMO_TABLE = "arxiv_papers"
CHECKPOINT_PARAM = "/arxiv/ingestion/checkpoint"

# CLIENTS
s3 = boto3.client("s3", region_name=REGION)
bedrock = boto3.client("bedrock-runtime", region_name=REGION)
s3vectors = boto3.client("s3vectors", region_name=REGION)
dynamodb = boto3.resource("dynamodb", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)
table = dynamodb.Table(DYNAMO_TABLE)

def get_titan_embedding(text):
    try:
        payload = {"inputText": text}
        response = bedrock.invoke_model(
            modelId="amazon.titan-embed-text-v2:0", 
            body=json.dumps(payload)
        )
        result = json.loads(response['body'].read())
        return result["embedding"]
    
    except Exception as e:
        print(f"Embedding Error: {e}")
        return None

def get_checkpoint():
    """Fetch the last processed line number from SSM."""
    try:
        response = ssm.get_parameter(Name=CHECKPOINT_PARAM)
        return int(response['Parameter']['Value'])
    except Exception as e:
        print(f"SSM Read Error (Defaulting to 0): {e}")
        return 0

def update_checkpoint(new_value):
    """Update the checkpoint in SSM."""
    ssm.put_parameter(
        Name=CHECKPOINT_PARAM,
        Value=str(new_value),
        Type='String',
        Overwrite=True
    )

def check_if_exists(paper_id):
    """Check if paper exists in DynamoDB."""
    try:
        resp = table.get_item(Key={'paper_id': paper_id})
        return 'Item' in resp
    except ClientError as e:
        print(f"DynamoDB Check Error: {e}")
        return False

def lambda_handler(event, context):
    # Get current checkpoint position
    start_line = get_checkpoint()
    print(f"Starting ingestion from line: {start_line}")

    # Stream file from S3
    try:
        obj = s3.get_object(Bucket=RAW_BUCKET, Key=RAW_KEY)
        line_iterator = obj['Body'].iter_lines()
    
    except Exception as e:
        return {"statusCode": 500, "body": f"S3 Read Error: {str(e)}"}

    current_line_idx = 0
    papers_processed = 0
    target_process_count = 1  # How many new papers to ingest per minute

    # 3. Iterate to find the new paper
    for line in line_iterator:
        # Skip lines we already processed
        if current_line_idx < start_line:
            current_line_idx += 1
            continue


        try:
            paper = json.loads(line)
            pid = str(paper.get("id")) if paper.get("id") else str(uuid.uuid4())

            # Check duplication
            if check_if_exists(pid):
                print(f"Skipping {pid} (Already exists)")
                current_line_idx += 1
                # We update checkpoint even if skipped so we don't check it again
                start_line += 1 
                continue

            # Process New Paper
            if paper.get("abstract"):
                print(f"Ingesting new paper: {pid}")
                
                # A. Generate Embedding
                emb = get_titan_embedding(paper["abstract"])
                
                if emb:
                    # B. Upload to S3 Vectors
                    vector_record = {
                        "key": pid,
                        "data": {"float32": emb},
                        "metadata": {
                            "category": paper.get("categories", "unknown").split(" ")[0],
                            "year": paper.get("update_date", "0000")[:4]
                        }
                    }
                    s3vectors.put_vectors(
                        vectorBucketName=VECTOR_BUCKET,
                        indexName=INDEX_NAME,
                        vectors=[vector_record]
                    )

                    # C. Upload to DynamoDB
                    dynamo_record = {
                        "paper_id": pid,
                        "title": paper.get("title", "Unknown"),
                        "abstract": paper.get("abstract", ""),
                        "authors": str(paper.get("authors_parsed", [])),
                        "date": paper.get("update_date", ""),
                        "categories": paper.get("categories", "")
                    }
                    table.put_item(Item=dynamo_record)
                    
                    papers_processed += 1

            # Update counters
            current_line_idx += 1
            start_line += 1 # Move checkpoint forward

            # Stop after ingesting the target amount (1 paper)
            if papers_processed >= target_process_count:
                break

        except Exception as e:
            print(f"Error processing line {current_line_idx}: {e}")
            current_line_idx += 1


    update_checkpoint(start_line)
    
    return {
        "statusCode": 200, 
        "body": f"Ingested {papers_processed} papers. New Checkpoint: {start_line}"
    }
