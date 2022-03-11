import argparse
import json
import os
import requests

JOBS_DIR = os.path.dirname(os.path.abspath(__file__))

def get_running_jobs(host:str, token: str):
    hed = {'Authorization': 'Bearer ' + token}
    url = f'{host}/api/2.1/jobs/list'

    resp = requests.get(url, headers=hed)
    data = resp.json()

    jobs = [(i['job_id'], i['settings']['name'] ) for i in  data['jobs']]
    
    return jobs

def get_repo_jobs():
    return [i.replace(".json","") for i in os.listdir(JOBS_DIR) if i.endswith(".json")]

def update_job(data: dict, host: str, token: str):
    hed = {'Authorization': 'Bearer ' + token}
    url = f'{host}/api/2.1/jobs/reset'
    
    resp = requests.post(url, headers=hed, json=data)
    return resp

def create_job(data: dict, host: str, token: str):
    hed = {'Authorization': 'Bearer ' + token}
    url = f'{host}/api/2.1/jobs/create'
    
    resp = requests.post(url, headers=hed, json=data)
    return resp

def import_json_job(name: str):
    path = os.path.join(JOBS_DIR, f'{name}.json')
    
    with open(path, 'r') as open_file:
        data = json.load(open_file)

    return data

def format_create(data: dict):
    return data['settings']

def format_reset(data: dict, job_id: int):
    new_data = {
        'new_settings': data['settings'],
        'job_id': job_id,
        }
    
    return new_data

def process_create(job_name: str, host: str, token: str):
    job_settings = format_create(import_json_job(job_name))
    create_job(job_settings, host, token)
    print(f"Criando job {job_name}")
    return None

def process_update(job_id: int, job_name: str, repo_jobs: list, host: str, token: str):
    
    if job_name in repo_jobs:
        job_settings = format_reset(import_json_job(job_name), job_id)
        update_job(job_settings, host, token)
        repo_jobs.remove(job_name)
        print(f"Atualizando job {job_name}")
    
    return repo_jobs

def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("--token")
    parser.add_argument("--host")
    args = parser.parse_args()

    token = args.token
    host = args.host.split("?")[0].strip("/")

    running_jobs = get_running_jobs(host, token)
    repo_jobs = get_repo_jobs()

    for i in running_jobs:
        job_id, job_name = i
        repo_jobs = process_update(job_id, job_name, repo_jobs, host, token)

    for i in repo_jobs:
        process_create(i, host, token)
        
if __name__ == "__main__":
    main()
