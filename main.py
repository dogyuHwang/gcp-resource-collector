import os
import json
import pandas as pd
from io import BytesIO
from google.cloud import compute_v1
from google.cloud import storage
from collections import defaultdict
from datetime import datetime

class GCPResourceCollector:
    def __init__(self, project_id):
        self.project_id = project_id
        self.compute_client = compute_v1.InstancesClient()
        self.disk_client = compute_v1.DisksClient()
        self.snapshot_client = compute_v1.SnapshotsClient()
        self.storage_client = storage.Client()
    
    def get_compute_resources(self):
        """Compute Engine 인스턴스별 CPU/Memory 집계"""
        resources = defaultdict(lambda: {'cpu': 0, 'memory': 0})
        
        try:
            zones_client = compute_v1.ZonesClient()
            zones = zones_client.list(project=self.project_id)
            
            for zone in zones:
                instances = self.compute_client.list(
                    project=self.project_id, 
                    zone=zone.name
                )
                
                for instance in instances:
                    if instance.status == 'RUNNING':
                        machine_type = instance.machine_type.split('/')[-1]
                        series = machine_type.split('-')[0]
                        
                        if series in ['e2', 'n2', 'n1', 'c2', 'c3']:
                            if 'micro' in machine_type:
                                cpu_count = 1
                                memory_gb = 1
                            elif 'small' in machine_type:
                                cpu_count = 1
                                memory_gb = 1.7
                            elif 'medium' in machine_type:
                                cpu_count = 1
                                memory_gb = 4
                            else:
                                parts = machine_type.split('-')
                                if len(parts) >= 3:
                                    cpu_count = int(parts[2])
                                    if 'standard' in machine_type:
                                        memory_gb = cpu_count * 3.75
                                    elif 'highmem' in machine_type:
                                        memory_gb = cpu_count * 6.5
                                    elif 'highcpu' in machine_type:
                                        memory_gb = cpu_count * 0.9
                                    else:
                                        memory_gb = cpu_count * 4
                                else:
                                    cpu_count = 2
                                    memory_gb = 7.5
                            
                            resources[f"{series}_cpu"] += cpu_count
                            resources[f"{series}_memory"] += memory_gb
                            
        except Exception as e:
            print(f"Compute Engine 리소스 수집 오류: {e}")
        
        return dict(resources)
    
    def get_disk_resources(self):
        """디스크 타입별 용량 집계"""
        disk_usage = defaultdict(float)
        
        try:
            zones_client = compute_v1.ZonesClient()
            zones = zones_client.list(project=self.project_id)
            
            for zone in zones:
                disks = self.disk_client.list(
                    project=self.project_id,
                    zone=zone.name
                )
                
                for disk in disks:
                    disk_type = disk.type.split('/')[-1]
                    size_gb = disk.size_gb
                    disk_usage[disk_type] += size_gb
                    
        except Exception as e:
            print(f"디스크 리소스 수집 오류: {e}")
        
        return dict(disk_usage)
    
    def get_snapshot_usage(self):
        """스냅샷 총 용량"""
        total_snapshot_gb = 0
        
        try:
            snapshots = self.snapshot_client.list(project=self.project_id)
            for snapshot in snapshots:
                if hasattr(snapshot, 'storage_bytes'):
                    total_snapshot_gb += snapshot.storage_bytes / (1024**3)
                    
        except Exception as e:
            print(f"스냅샷 리소스 수집 오류: {e}")
        
        return total_snapshot_gb
    
    def get_gcs_usage(self):
        """GCS 버킷별 용량"""
        gcs_usage = {}
        total_gcs_gb = 0
        
        try:
            buckets = self.storage_client.list_buckets()
            for bucket in buckets:
                bucket_size = 0
                blobs = self.storage_client.list_blobs(bucket.name)
                for blob in blobs:
                    if blob.size:
                        bucket_size += blob.size
                
                bucket_size_gb = bucket_size / (1024**3)
                gcs_usage[bucket.name] = bucket_size_gb
                total_gcs_gb += bucket_size_gb
                
        except Exception as e:
            print(f"GCS 리소스 수집 오류: {e}")
        
        gcs_usage['total_gcs_gb'] = total_gcs_gb
        return gcs_usage

def save_to_excel_gcs(result_data, bucket_name=None):
    """결과를 엑셀 파일로 GCS에 저장"""
    if bucket_name is None:
        bucket_name = os.environ.get('BUCKET_NAME')
        if not bucket_name:
            print("ERROR: BUCKET_NAME 환경변수가 설정되지 않았습니다")
            return None
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        
        excel_buffer = BytesIO()
        
        with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
            if result_data['compute_resources']:
                compute_df = pd.DataFrame([
                    {'Resource Type': k, 'Value': v} 
                    for k, v in result_data['compute_resources'].items()
                ])
                compute_df.to_excel(writer, sheet_name='Compute_Resources', index=False)
            
            if result_data['disk_usage_gb']:
                disk_df = pd.DataFrame([
                    {'Disk Type': k, 'Size (GB)': v} 
                    for k, v in result_data['disk_usage_gb'].items()
                ])
                disk_df.to_excel(writer, sheet_name='Disk_Usage', index=False)
            
            if result_data['gcs_usage']:
                gcs_df = pd.DataFrame([
                    {'Bucket Name': k, 'Size (GB)': v} 
                    for k, v in result_data['gcs_usage'].items()
                ])
                gcs_df.to_excel(writer, sheet_name='GCS_Usage', index=False)
            
            summary_data = [
                ['Project ID', result_data['project_id']],
                ['Snapshot Total (GB)', result_data['snapshot_total_gb']],
                ['Collection Time', result_data['timestamp']]
            ]
            summary_df = pd.DataFrame(summary_data, columns=['Item', 'Value'])
            summary_df.to_excel(writer, sheet_name='Summary', index=False)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"gcp_resources_{result_data['project_id']}_{timestamp}.xlsx"
        
        excel_buffer.seek(0)
        blob = bucket.blob(filename)
        blob.upload_from_file(excel_buffer, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        
        print(f"✓ 엑셀 파일 저장 완료: gs://{bucket_name}/{filename}")
        return filename
        
    except Exception as e:
        print(f"엑셀 파일 저장 오류: {e}")
        return None

def main():
    """1회성 리소스 수집 실행"""
    project_id = os.environ.get('PROJECT_ID')
    if not project_id:
        print("ERROR: PROJECT_ID 환경변수가 설정되지 않았습니다")
        return
    
    print(f"프로젝트 {project_id}의 리소스 수집 중...")
    
    try:
        collector = GCPResourceCollector(project_id)
        
        print("Compute Engine 리소스 수집...")
        compute_resources = collector.get_compute_resources()
        
        print("디스크 리소스 수집...")
        disk_resources = collector.get_disk_resources()
        
        print("스냅샷 리소스 수집...")
        snapshot_usage = collector.get_snapshot_usage()
        
        print("GCS 리소스 수집...")
        gcs_usage = collector.get_gcs_usage()
        
        result = {
            'project_id': project_id,
            'compute_resources': compute_resources,
            'disk_usage_gb': disk_resources,
            'snapshot_total_gb': snapshot_usage,
            'gcs_usage': gcs_usage,
            'timestamp': datetime.utcnow().isoformat()
        }
        
        print("=" * 50)
        print("GCP 리소스 수집 결과")
        print("=" * 50)
        print(json.dumps(result, indent=2, ensure_ascii=False))
        
        print("\nGCS에 엑셀 파일 저장 중...")
        filename = save_to_excel_gcs(result)
        
        if filename:
            print(f"✓ 수집 완료: dogyu-test 버킷에 {filename} 저장됨")
        else:
            print("✗ 파일 저장 실패")
        
    except Exception as e:
        print(f"ERROR: {str(e)}")

if __name__ == '__main__':
    main()