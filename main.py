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
    
    def get_machine_specs(self, machine_type):
        """N2, E2 시리즈 전용 정확한 CPU/메모리 스펙"""
        
        # E2 공유 코어 타입들
        if machine_type == 'e2-micro':
            return (0.25, 1)
        elif machine_type == 'e2-small':
            return (0.5, 2)
        elif machine_type == 'e2-medium':
            return (1, 4)
        
        # 패턴 기반 계산
        parts = machine_type.split('-')
        if len(parts) < 3:
            return (2, 8)  # 기본값
            
        series = parts[0]  # e2, n2
        type_name = parts[1]  # standard, highmem, highcpu
        
        try:
            cpu_count = int(parts[2])
        except ValueError:
            return (2, 8)
        
        # N2, E2 시리즈만 처리
        if series == 'e2':
            if 'standard' in machine_type:
                memory_gb = cpu_count * 4     # vCPU당 4GB
            elif 'highmem' in machine_type:
                memory_gb = cpu_count * 8     # vCPU당 8GB
            elif 'highcpu' in machine_type:
                memory_gb = cpu_count * 1     # vCPU당 1GB
            else:
                memory_gb = cpu_count * 4
                
        elif series == 'n2':
            if 'standard' in machine_type:
                memory_gb = cpu_count * 4     # vCPU당 4GB
            elif 'highmem' in machine_type:
                memory_gb = cpu_count * 8     # vCPU당 8GB
            elif 'highcpu' in machine_type:
                memory_gb = cpu_count * 1     # vCPU당 1GB
            else:
                memory_gb = cpu_count * 4
        else:
            # n2, e2가 아닌 시리즈는 무시
            return None
        
        return (cpu_count, memory_gb)
    
    def get_compute_resources(self):
        """Compute Engine 인스턴스별 CPU/Memory 집계 (Running/Stopped 구분)"""
        running_resources = defaultdict(int)
        stopped_resources = defaultdict(int)
        
        try:
            zones_client = compute_v1.ZonesClient()
            zones = zones_client.list(project=self.project_id)
            
            for zone in zones:
                instances = self.compute_client.list(
                    project=self.project_id, 
                    zone=zone.name
                )
                
                for instance in instances:
                    machine_type = instance.machine_type.split('/')[-1]
                    series = machine_type.split('-')[0]
                    
                    cpu_count, memory_gb = self.get_machine_specs(machine_type)
                    
                    if instance.status == 'RUNNING':
                        running_resources[f"{series}_cpu"] += cpu_count
                        running_resources[f"{series}_memory"] += memory_gb
                    else:  # STOPPED, TERMINATED 등
                        stopped_resources[f"{series}_cpu"] += cpu_count
                        stopped_resources[f"{series}_memory"] += memory_gb
                        
        except Exception as e:
            print(f"Compute Engine 리소스 수집 오류: {e}")
        
        return dict(running_resources), dict(stopped_resources)
    
    def get_disk_resources(self):
        """디스크 타입별 용량 집계"""
        disk_usage = defaultdict(int)
        
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
                    size_gb = int(disk.size_gb)
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
                    total_snapshot_gb += int(snapshot.storage_bytes / (1024**3))
                    
        except Exception as e:
            print(f"스냅샷 리소스 수집 오류: {e}")
        
        return total_snapshot_gb
    
    def get_gcs_usage(self):
        """GCS 버킷별 용량 (GB 단위)"""
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
                
                bucket_size_gb = int(bucket_size / (1024**3))
                gcs_usage[bucket.name] = bucket_size_gb
                total_gcs_gb += bucket_size_gb
                
        except Exception as e:
            print(f"GCS 리소스 수집 오류: {e}")
        
        gcs_usage['total_gcs_gb'] = total_gcs_gb
        return gcs_usage

def save_to_excel_gcs(result_data, bucket_name=None):
    """결과를 엑셀 파일로 GCS에 저장 (단일 시트)"""
    if bucket_name is None:
        bucket_name = os.environ.get('BUCKET_NAME')
        if not bucket_name:
            print("ERROR: BUCKET_NAME 환경변수가 설정되지 않았습니다")
            return None
    
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        
        # 모든 데이터를 하나의 리스트로 정리
        all_data = []
        
        # 헤더
        all_data.append(['Category', 'Instance/Resource', 'Zone', 'Machine Type', 'Status', 'CPU', 'Memory(GB)', 'PD-Standard(GB)', 'PD-Balanced(GB)', 'PD-SSD(GB)', 'Local-SSD(GB)'])
        
        # 프로젝트 정보
        all_data.append(['Project Info', result_data['project_id'], '', '', '', '', '', '', '', '', ''])
        all_data.append(['Collection Time', result_data['timestamp'], '', '', '', '', '', '', '', '', ''])
        all_data.append(['', '', '', '', '', '', '', '', '', '', ''])  # 빈 줄
        
        # 인스턴스별 상세 정보
        for instance in result_data['instances']:
            all_data.append([
                'Instance',
                instance['name'],
                instance['zone'],
                instance['machine_type'],
                instance['status'],
                instance['cpu'],
                instance['memory_gb'],
                instance['disks']['pd-standard'],
                instance['disks']['pd-balanced'],
                instance['disks']['pd-ssd'],
                instance['disks']['local-ssd']
            ])
        
        all_data.append(['', '', '', '', '', '', '', '', '', '', ''])  # 빈 줄
        
        # 스냅샷
        all_data.append(['Snapshot', 'Total Snapshots', '', '', '', '', result_data['snapshot_total_gb'], '', '', '', ''])
        all_data.append(['', '', '', '', '', '', '', '', '', '', ''])  # 빈 줄
        
        # GCS Usage
        all_data.append(['GCS', 'Bucket Name', 'Size(GB)', '', '', '', '', '', '', '', ''])
        for bucket_name_item, size_gb in result_data['gcs_usage'].items():
            all_data.append(['GCS', bucket_name_item, size_gb, '', '', '', '', '', '', '', ''])
        
        # DataFrame 생성 및 엑셀 저장
        df = pd.DataFrame(all_data)
        
        excel_buffer = BytesIO()
        df.to_excel(excel_buffer, index=False, header=False, engine='openpyxl')
        
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
        instances = collector.get_compute_resources()
        
        print("스냅샷 리소스 수집...")
        snapshot_usage = collector.get_snapshot_usage()
        
        print("GCS 리소스 수집...")
        gcs_usage = collector.get_gcs_usage()
        
        result = {
            'project_id': project_id,
            'instances': instances,
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
            print(f"✓ 수집 완료: 버킷에 {filename} 저장됨")
        else:
            print("✗ 파일 저장 실패")
        
    except Exception as e:
        print(f"ERROR: {str(e)}")

if __name__ == '__main__':
    main()