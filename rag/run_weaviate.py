import weaviate
from weaviate.classes.init import Auth
from weaviate.classes.config import Configure
import os, json
import argparse
from scrape import run_crawler_faq_site

def connect_to_weaviate():
    weaviate_url = os.environ["WEAVIATE_URL"]
    weaviate_api_key = os.environ["WEAVIATE_API_KEY"]
    client = weaviate.connect_to_weaviate_cloud(
        cluster_url=weaviate_url,
        auth_credentials=Auth.api_key(weaviate_api_key),
    )
    print(client.is_ready()) # Should print: `True`
    return client

def create_weaviate_collection(client, collection_name="Question"):
    questions = client.collections.create(
        name=collection_name,
        vectorizer_config=Configure.Vectorizer.text2vec_weaviate(), # Configure the Weaviate Embeddings integration
        generative_config=Configure.Generative.cohere()             # Configure the Cohere generative AI integration
    )
    return questions

def import_data_to_weaviate(data, collection):
    with collection.batch.dynamic() as batch:
        for d in data:
            batch.add_object({
                "category": d["category"],
                "question": d["question"],
                "answer": d["answer"],
            })
            if batch.number_errors > 10:
                print("Batch import stopped due to excessive errors.")
                break
    failed_objects = collection.batch.failed_objects
    if failed_objects:
        print(f"Number of failed imports: {len(failed_objects)}")
        print(f"First failed object: {failed_objects[0]}")

def read_data_from_weaviate(text, collection):
    response = collection.query.near_text(
        query=text,
        limit=2
    )
    for obj in response.objects:
        print(json.dumps(obj.properties, indent=2))

def run_import(collection):
    ## Random test data
    # data = [{
    #     "category": "General",
    #     "question": "What is the capital of France?",
    #     "answer": "Paris"
    # }]
    data = run_crawler_faq_site()
    import_data_to_weaviate(data, collection)

def run_read(client, collection_name, query="Capital"):
    questions_collection = client.collections.get(collection_name)
    read_data_from_weaviate(query, questions_collection)
    client.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Manage Weaviate operations via CLI.")
    parser.add_argument("--create", action="store_true", help="Create the Weaviate collection.")
    parser.add_argument("--import_data", action="store_true", help="Import data into Weaviate.")
    parser.add_argument("--read", type=str, metavar="QUERY", help="Read data from Weaviate with a query.")

    args = parser.parse_args()
    client = connect_to_weaviate()

    if args.create:
        questions_collection = create_weaviate_collection(client, "faq")
    elif args.import_data:
        questions_collection = client.collections.get("faq")
        run_import(questions_collection)
    elif args.read:
        run_read(client, "faq", "in person")
    else:
        print("No valid option selected. Use --help for usage instructions.")
    client.close()

"""
=========================
Example CLI Commands
=========================
1) Create the collection:
   python3 run_weaviate.py --create

2) Import data:
   python3 run_weaviate.py --import_data

3) Read data with a query:
   python3 run_weaviate.py --read "in person"
=========================
"""