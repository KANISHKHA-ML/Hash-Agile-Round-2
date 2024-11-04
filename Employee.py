from elasticsearch import Elasticsearch, helpers
import csv

es_client = Elasticsearch(["http://localhost:8989"])

def create_index(index_name):
    if not es_client.indices.exists(index=index_name):
        es_client.indices.create(index=index_name)
        print(f"Index '{index_name}' created.")
    else:
        print(f"Index '{index_name}' already exists.")

def index_data(index_name, exclude_column):
    with open('employee_data.csv', mode='r') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        bulk_actions = []
        
        for record in csv_reader:
            record.pop(exclude_column, None)
            bulk_actions.append({
                "_index": index_name,
                "_source": record
            })
        
        helpers.bulk(es_client, bulk_actions)
        print(f"Data indexed into '{index_name}' excluding column '{exclude_column}'.")

def search_by_field(index_name, field_name, field_value):
    search_query = {
        "query": {
            "match": {
                field_name: field_value
            }
        }
    }
    search_results = es_client.search(index=index_name, body=search_query)
    print(f"Search results for {field_name}='{field_value}':")
    for hit in search_results['hits']['hits']:
        print(hit['_source'])

def get_employee_count(index_name):
    employee_count = es_client.count(index=index_name)['count']
    print(f"Total employee count in '{index_name}': {employee_count}")

def delete_employee_by_id(index_name, employee_id):
    delete_query = {
        "query": {
            "match": {
                "employee_id": employee_id
            }
        }
    }
    es_client.delete_by_query(index=index_name, body=delete_query)
    print(f"Employee with ID '{employee_id}' deleted from '{index_name}'.")

def get_department_facet(index_name):
    facet_query = {
        "size": 0,
        "aggs": {
            "department_counts": {
                "terms": {
                    "field": "department.keyword"
                }
            }
        }
    }
    facet_results = es_client.search(index=index_name, body=facet_query)
    print("Employee count by department:")
    for bucket in facet_results['aggregations']['department_counts']['buckets']:
        print(f"Department: {bucket['key']}, Count: {bucket['doc_count']}")

index_name_employee = 'Hash_JohnDoe'
index_name_phone = 'Hash_1234'

create_index(index_name_employee)
create_index(index_name_phone)

get_employee_count(index_name_employee)

index_data(index_name_employee, 'Department')
index_data(index_name_phone, 'Gender')

get_employee_count(index_name_employee)

delete_employee_by_id(index_name_employee, 'E02003')

get_employee_count(index_name_employee)

search_by_field(index_name_employee, 'Department', 'IT')
search_by_field(index_name_employee, 'Gender', 'Male')
search_by_field(index_name_phone, 'Department', 'IT')

get_department_facet(index_name_employee)
get_department_facet(index_name_phone)
