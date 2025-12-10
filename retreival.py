import boto3
import json
import time
from botocore.exceptions import ClientError

# CONFIG 
REGION = "us-east-2"
VECTOR_BUCKET = "kaggle-arxiv-vector-bucket"
INDEX_NAME = "arxiv-index"
DYNAMO_TABLE = "arxiv_papers"

# SET CLIENTS 
bedrock = boto3.client("bedrock-runtime", region_name=REGION)
s3vectors = boto3.client("s3vectors", region_name=REGION)
dynamodb = boto3.resource("dynamodb", region_name=REGION)

class ArxivRetriever:
    def __init__(self, bedrock_client, dynamo_resource, vector_client, table_name):
        self.bedrock = bedrock_client
        self.dynamodb = dynamo_resource
        self.s3vectors = vector_client
        self.table_name = table_name

    def _get_embedding(self, text):
        try:
            response = self.bedrock.invoke_model(
                modelId="amazon.titan-embed-text-v2:0",
                body=json.dumps({"inputText": text})
            )
            return json.loads(response['body'].read())["embedding"]
        except Exception as e:
            print(f"⚠️ Embedding Error: {e}")
            return None

    def _fetch_from_dynamo_batch(self, paper_ids):
        if not paper_ids: return {}
        try:
            response = self.dynamodb.batch_get_item(
                RequestItems={
                    self.table_name: {'Keys': [{'paper_id': pid} for pid in paper_ids]}
                }
            )
            fetched_items = response['Responses'].get(self.table_name, [])
            return {item['paper_id']: item for item in fetched_items}
        except ClientError as e:
            print(f"❌ DynamoDB Error: {e}")
            return {}

    def search(self, query: str, k: int = 3):

        t0 = time.time()
        query_emb = self._get_embedding(query)
        t1 = time.time()
        embed_time = t1 - t0
        print(f"Query Embedding Time:  {embed_time:.4f} seconds")
        if not query_emb: return []


        t2 = time.time()
        try:
            # --- QUERY ---
            response = self.s3vectors.query_vectors(
                vectorBucketName=VECTOR_BUCKET,
                indexName=INDEX_NAME,
                queryVector={"float32": query_emb},
                topK=k,
                returnDistance=True,
                returnMetadata=True
            )
        except Exception as e:
            print(f"❌ Vector Search Error: {e}")
            return []
        t3 = time.time()
        search_time = t3 - t2
        print(f"S3 Vector Search Time:  {search_time:.4f} seconds")
        # --- FIX: Extract the list from the dictionary ---
        # The API returns {'vectors': [...], ...}, not just [...]
        vector_list = response.get("vectors", [])

        if not vector_list:
            print("   No matches found.")
            return []

        # Extract IDs using the 'key' field (as set in our Ingestion logic)
        top_ids = [res.get('key') for res in vector_list if res.get('key')]
        print(f"   Found IDs: {top_ids}")

       
        t4 = time.time()
        content_map = self._fetch_from_dynamo_batch(top_ids)
        t5 = time.time()
        dynamo_time = t5 - t4
        print(f"DB Content Fetch Time: {dynamo_time:.4f} seconds")

        # 4. Join Data
        final_results = []
        for res in vector_list:
            pid = res.get('key')
            
            # Robust Score Extraction (Handles both 'distance' and 'score')
            # For Titan embeddings: Cosine Similarity is usually implied by distance
            dist = res.get('distance')
            score = res.get('score')
            final_score = dist if dist is not None else (score if score is not None else 0.0)

            if pid in content_map:
                record = content_map[pid]
                final_results.append({
                    "id": pid,
                    "score": final_score,
                    "title": record.get("title", "No Title"),
                    "abstract": record.get("abstract", "No Abstract"),
                    "date": record.get("date", "Unknown"),
                    "authors": record.get("authors", "[]")
                })
            else:
                print(f"   ⚠️ Warning: ID {pid} found in Vector Store but missing in DynamoDB.")

        return final_results

# Instantiate
def return_retriever():
    retriever = ArxivRetriever(bedrock, dynamodb, s3vectors, DYNAMO_TABLE)
    return retriever
