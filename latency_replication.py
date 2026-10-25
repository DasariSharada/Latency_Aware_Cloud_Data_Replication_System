#!/usr/bin/env python3
"""
latency_replication.py

Latency-Aware Cloud Data Replication System
- Measure latency to a list of datacenter endpoints
- Choose best datacenter (lowest median latency)
- Upload file to chosen datacenter (AWS S3 / Azure Blob / local simulation)

Usage:
    python latency_replication.py --file /path/to/file --config datacenters.json --provider aws
"""

import argparse
import os
import socket
import time
import statistics
import json
import logging
from pathlib import Path
from typing import List, Dict, Any

# Optional cloud libs
try:
    import boto3
    from botocore.exceptions import BotoCoreError, ClientError
except Exception:
    boto3 = None

try:
    from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
except Exception:
    BlobServiceClient = None

# -------------------------
# Logging
# -------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger(__name__)

# -------------------------
# Latency measurement utils
# -------------------------
def measure_tcp_connect_rtt(host: str, port: int = 443, timeout: float = 2.0) -> float:
    """
    Perform a TCP connect and return round-trip connect time in milliseconds.
    Returns float('inf') on failure.
    """
    start = time.time()
    try:
        with socket.create_connection((host, port), timeout=timeout) as sock:
            pass
        elapsed = (time.time() - start) * 1000.0
        return elapsed
    except Exception as e:
        # Connection failed, treat as very high latency
        return float('inf')

def measure_http_get_rtt(url: str, timeout: float = 3.0) -> float:
    """
    Measure HTTP GET latency. Uses requests if available, else fallback to TCP connect to host.
    Returns milliseconds or inf on failure.
    """
    try:
        import requests
    except Exception:
        # requests not available; use TCP connect fallback
        host = url.split("//")[-1].split("/")[0]
        return measure_tcp_connect_rtt(host, 443, timeout)

    start = time.time()
    try:
        r = requests.get(url, timeout=timeout)
        elapsed = (time.time() - start) * 1000.0
        return elapsed
    except Exception:
        return float('inf')

def measure_latency(endpoint: Dict[str, Any], trials: int = 5, method: str = "tcp") -> float:
    """
    endpoint: { 'name': 'us-east', 'host': 'example.com', 'url': 'https://example.com/ping' }
    method: 'tcp' or 'http'
    Returns median RTT in milliseconds across trials.
    """
    results = []
    host = endpoint.get('host')
    url = endpoint.get('url')

    for i in range(trials):
        if method == "http" and url:
            rtt = measure_http_get_rtt(url)
        else:
            if not host:
                logger.debug("No host for endpoint; returning inf")
                rtt = float('inf')
            else:
                # default TCP connect to port 443
                rtt = measure_tcp_connect_rtt(host, port=endpoint.get('port', 443))
        results.append(rtt)
        time.sleep(0.12)  # slight spacing between trials

    # Filter out infinite (failures) before median; if all failed, return inf
    finite = [r for r in results if r != float('inf')]
    if not finite:
        return float('inf')
    median = statistics.median(finite)
    return median

# -------------------------
# Cloud upload helpers
# -------------------------
def upload_to_aws_s3(file_path: str, bucket: str, key: str = None, aws_profile: str = None) -> bool:
    if boto3 is None:
        logger.error("boto3 not installed. Install with `pip install boto3` to use AWS upload.")
        return False
    if key is None:
        key = os.path.basename(file_path)
    try:
        session = boto3.Session(profile_name=aws_profile) if aws_profile else boto3
        s3 = session.client('s3') if isinstance(session, boto3.Session) else boto3.client('s3')
        logger.info(f"Uploading {file_path} to s3://{bucket}/{key} ...")
        s3.upload_file(file_path, bucket, key)
        logger.info("Upload to AWS S3 completed.")
        return True
    except Exception as e:
        logger.exception("AWS S3 upload failed:")
        return False

def upload_to_azure_blob(file_path: str, container: str, blob_name: str = None, connection_string: str = None) -> bool:
    if BlobServiceClient is None:
        logger.error("azure-storage-blob not installed. Install with `pip install azure-storage-blob` to use Azure upload.")
        return False
    if blob_name is None:
        blob_name = os.path.basename(file_path)
    try:
        if connection_string:
            svc = BlobServiceClient.from_connection_string(conn_str=connection_string)
        else:
            # fallback to default authentication (env vars)
            svc = BlobServiceClient.from_connection_string(os.getenv("AZURE_STORAGE_CONNECTION_STRING"))
            if svc is None:
                logger.error("No Azure connection string found in environment.")
                return False
        container_client = svc.get_container_client(container)
        # create container if not exists
        try:
            container_client.create_container()
        except Exception:
            pass
        blob_client = container_client.get_blob_client(blob_name)
        logger.info(f"Uploading {file_path} to Azure container {container}/{blob_name} ...")
        with open(file_path, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)
        logger.info("Upload to Azure Blob completed.")
        return True
    except Exception as e:
        logger.exception("Azure Blob upload failed:")
        return False

def replicate_locally(file_path: str, datacenter_name: str, base_dir: str = "./replicated") -> bool:
    """
    Simple local replication (for simulation): copy file to base_dir/datacenter_name/
    """
    try:
        dest_dir = Path(base_dir) / datacenter_name
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest_path = dest_dir / Path(file_path).name
        with open(file_path, "rb") as src, open(dest_path, "wb") as dst:
            dst.write(src.read())
        logger.info(f"Local replication complete: {dest_path}")
        return True
    except Exception:
        logger.exception("Local replication failed:")
        return False

# -------------------------
# Orchestration
# -------------------------
def select_best_datacenter(datacenters: List[Dict[str, Any]], trials: int = 5, method: str = "tcp") -> Dict[str, Any]:
    """
    Measure median latency for each datacenter and return the best one.
    Each datacenter dict should at least have: { 'name': 'us-east', 'host': 'example.com', ... }
    """
    logger.info("Measuring latency to datacenters...")
    results = []
    for dc in datacenters:
        name = dc.get('name', '<unnamed>')
        logger.info(f"Probing {name} ({dc.get('host') or dc.get('url')}) ...")
        median_rtt = measure_latency(dc, trials=trials, method=method)
        logger.info(f"  {name}: median RTT = {'inf' if median_rtt==float('inf') else f'{median_rtt:.2f} ms'}")
        results.append((median_rtt, dc))
    # sort by latency
    results.sort(key=lambda x: x[0])
    best_latency, best_dc = results[0]
    if best_latency == float('inf'):
        logger.warning("All datacenter probes failed. No reachable datacenter found.")
        return {}
    logger.info(f"Selected datacenter: {best_dc.get('name')} with median RTT {best_latency:.2f} ms")
    # attach measured latency for reference
    best_dc['_measured_latency_ms'] = best_latency
    return best_dc

def replicate_file_to_datacenter(file_path: str, datacenter: Dict[str, Any], provider: str, aws_profile: str = None, azure_conn: str = None) -> bool:
    """
    datacenter should include provider-specific target information:
    - for aws: {'target_bucket': 'mybucket', 'target_key_prefix': 'path/'}
    - for azure: {'target_container': 'container-name', 'target_blob_prefix': 'path/'}
    - for local simulation: just 'name'
    """
    provider = provider.lower()
    if provider == 'aws':
        bucket = datacenter.get('target_bucket')
        if not bucket:
            logger.error("Datacenter config missing 'target_bucket' for AWS.")
            return False
        key_prefix = datacenter.get('target_key_prefix', '')
        key = key_prefix + Path(file_path).name
        return upload_to_aws_s3(file_path, bucket, key, aws_profile=aws_profile)
    elif provider == 'azure':
        container = datacenter.get('target_container')
        if not container:
            logger.error("Datacenter config missing 'target_container' for Azure.")
            return False
        blob_prefix = datacenter.get('target_blob_prefix', '')
        blob_name = blob_prefix + Path(file_path).name
        return upload_to_azure_blob(file_path, container, blob_name, connection_string=azure_conn)
    elif provider == 'local' or provider == 'simulate':
        return replicate_locally(file_path, datacenter.get('name', 'dc_unnamed'))
    else:
        logger.error(f"Unsupported provider: {provider}")
        return False

# -------------------------
# CLI + Main
# -------------------------
def load_datacenters_from_json(path: str) -> List[Dict[str, Any]]:
    with open(path, "r") as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise ValueError("Datacenters JSON should be a list of datacenter objects.")
    return data

def main():
    parser = argparse.ArgumentParser(description="Latency-Aware Cloud Data Replication")
    parser.add_argument("--file", required=True, help="Path to file to replicate")
    parser.add_argument("--config", required=True, help="JSON file with datacenter definitions")
    parser.add_argument("--provider", required=True, choices=['aws', 'azure', 'local', 'simulate'],
                        help="Cloud provider to replicate to (choose 'local' for simulation)")
    parser.add_argument("--trials", type=int, default=5, help="Number of latency trials per datacenter")
    parser.add_argument("--method", choices=['tcp', 'http'], default='tcp', help="Probe method")
    parser.add_argument("--aws-profile", default=None, help="AWS profile name (optional)")
    parser.add_argument("--azure-conn", default=None, help="Azure connection string (optional)")
    args = parser.parse_args()

    file_path = os.path.abspath(args.file)
    if not os.path.isfile(file_path):
        logger.error(f"File not found: {file_path}")
        return

    datacenters = load_datacenters_from_json(args.config)
    if not datacenters:
        logger.error("No datacenters defined.")
        return

    best = select_best_datacenter(datacenters, trials=args.trials, method=args.method)
    if not best:
        logger.error("No suitable datacenter selected. Aborting replication.")
        return

    success = replicate_file_to_datacenter(file_path, best, provider=args.provider, aws_profile=args.aws_profile, azure_conn=args.azure_conn)
    if success:
        logger.info("Replication completed successfully.")
    else:
        logger.error("Replication failed.")

if __name__ == "__main__":
    main()
