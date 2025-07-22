import os
import json
import pandas as pd
from io import BytesIO
from google.cloud import compute_v1
from google.cloud import storage
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
            
        custom_memory = int(parts[-1])
        
        try:
            cpu_count = int(parts[2])
        except ValueError:
            return (2, 8)
        
        if 'standard' in machine_type:
            memory_gb = cpu_count * 4     # vCPU당 4GB
        elif 'highmem' in machine_type:
            memory_gb = cpu_count * 8     # vCPU당 8GB
        elif 'highcpu' in machine_type:
            memory_gb = cpu_count * 1     # vCPU당 1GB
        elif 'custom' in machine_type:
            memory_gb = custom_memory / 1024     # Custom memory 계산
        else:
            memory_gb = cpu_count * 4
            
        
        return (cpu_count, memory_gb)
    
def get_compute_resources(self):
    """인스턴스별 상세 CPU/Memory/Disk/IP 정보 수집"""
    instances_info = []
   
    try:
        zones_client = compute_v1.ZonesClient()
        zones = zones_client.list(project=self.project_id)
       
        for zone in zones:
            print(f"Processing zone: {zone.name}")
            instances = self.compute_client.list(
                project=self.project_id,
                zone=zone.name
            )
           
            for instance in instances:
                # 인스턴스 기본 정보 추출
                instance_name = instance.name if hasattr(instance, 'name') else 'unknown'
                instance_status = instance.status if hasattr(instance, 'status') else 'unknown'
                machine_type = instance.machine_type.split('/')[-1]
                series = machine_type.split('-')[0]
               
                print(f"Processing instance: {instance_name} ({machine_type})")
               
                # n2, e2 시리즈만 처리
                if series not in ['n2', 'e2']:
                    print(f"Skipping {series} series")
                    continue
               
                specs = self.get_machine_specs(machine_type)
                if specs is None:
                    continue
                   
                cpu_count, memory_gb = specs
               
                # IP 주소 정보 수집
                private_ips = []
                public_ips = []
                
                if hasattr(instance, 'network_interfaces') and instance.network_interfaces:
                    for network_interface in instance.network_interfaces:
                        # Private IP 수집
                        if hasattr(network_interface, 'network_i_p') and network_interface.network_i_p:
                            private_ips.append(network_interface.network_i_p)
                        
                        # Public IP 수집 (External IP)
                        if hasattr(network_interface, 'access_configs') and network_interface.access_configs:
                            for access_config in network_interface.access_configs:
                                if hasattr(access_config, 'nat_i_p') and access_config.nat_i_p:
                                    public_ips.append(access_config.nat_i_p)
               
                # 인스턴스의 디스크 정보 수집
                disks_info = self.get_instance_disks(instance, zone.name)
               
                instance_data = {
                    'name': instance_name,
                    'zone': zone.name,
                    'machine_type': machine_type,
                    'status': instance_status,
                    'cpu': cpu_count,
                    'memory_gb': memory_gb,
                    'private_ips': ', '.join(private_ips) if private_ips else 'None',
                    'public_ips': ', '.join(public_ips) if public_ips else 'None',
                    'disks': disks_info
                }
               
                instances_info.append(instance_data)
                   
    except Exception as e:
        print(f"Compute Engine 리소스 수집 오류: {e}")
   
    return instances_info
    
def get_instance_disks(self, instance, zone_name):
    """인스턴스별 디스크 정보 수집 (정확한 계산)"""
    disks_info = {
        'pd-standard': 0.0,
        'pd-balanced': 0.0,
        'pd-ssd': 0.0,
        'local-ssd': 0.0
    }
    
    try:
        if hasattr(instance, 'disks') and instance.disks:
            for disk in instance.disks:
                if hasattr(disk, 'source') and disk.source:
                    # 영구 디스크인 경우
                    disk_name = disk.source.split('/')[-1]
                    try:
                        disk_detail = self.disk_client.get(
                            project=self.project_id,
                            zone=zone_name,
                            disk=disk_name
                        )
                        disk_type = disk_detail.type.split('/')[-1]
                        
                        # 정확한 크기 계산 (bytes -> GB)
                        if hasattr(disk_detail, 'size_gb'):
                            size_gb = float(disk_detail.size_gb)
                        elif hasattr(disk_detail, 'size_bytes'):
                            size_gb = float(disk_detail.size_bytes) / (1024**3)
                        else:
                            print(f"디스크 {disk_name} 크기 정보 없음")
                            continue
                        
                        # 반올림하여 소수점 2자리까지
                        size_gb = round(size_gb, 2)
                        
                        if disk_type in disks_info:
                            disks_info[disk_type] += size_gb
                        else:
                            print(f"알 수 없는 디스크 타입: {disk_type}")
                        
                    except Exception as e:
                        print(f"디스크 {disk_name} 정보 수집 오류: {e}")
                        
                elif hasattr(disk, 'type_') and disk.type_ == 'SCRATCH':
                    # 로컬 SSD인 경우
                    if hasattr(disk, 'disk_size_gb'):
                        local_ssd_size = float(disk.disk_size_gb)
                    else:
                        # 기본값: 375GB (GCP 로컬 SSD 기본 크기)
                        local_ssd_size = 375.0
                    
                    disks_info['local-ssd'] += round(local_ssd_size, 2)
                
    except Exception as e:
        print(f"인스턴스 디스크 정보 수집 오류: {e}")
    
    # 모든 값을 소수점 2자리로 반올림
    for key in disks_info:
        disks_info[key] = round(disks_info[key], 2)
    
    return disks_info

def get_snapshot_usage(self):
    """스냅샷 총 용량 (정확한 계산)"""
    total_snapshot_gb = 0.0
    
    try:
        snapshots = self.snapshot_client.list(project=self.project_id)
        for snapshot in snapshots:
            if hasattr(snapshot, 'storage_bytes') and snapshot.storage_bytes:
                # bytes를 GB로 정확히 변환
                snapshot_gb = float(snapshot.storage_bytes) / (1024**3)
                total_snapshot_gb += snapshot_gb
            elif hasattr(snapshot, 'disk_size_gb') and snapshot.disk_size_gb:
                # 이미 GB 단위인 경우
                total_snapshot_gb += float(snapshot.disk_size_gb)
                
    except Exception as e:
        print(f"스냅샷 리소스 수집 오류: {e}")
    
    return round(total_snapshot_gb, 2)

def get_gcs_usage(self):
    """GCS 버킷별 용량 (정확한 GB 계산)"""
    gcs_usage = {}
    total_gcs_gb = 0.0
    
    try:
        buckets = self.storage_client.list_buckets(project=self.project_id)
        for bucket in buckets:
            bucket_size_bytes = 0
            print(f"Processing bucket: {bucket.name}")
            
            try:
                blobs = self.storage_client.list_blobs(bucket.name)
                for blob in blobs:
                    if hasattr(blob, 'size') and blob.size:
                        bucket_size_bytes += int(blob.size)
            except Exception as e:
                print(f"버킷 {bucket.name} 처리 오류: {e}")
            
            # bytes를 GB로 정확히 변환
            bucket_size_gb = round(float(bucket_size_bytes) / (1024**3), 2)
            gcs_usage[bucket.name] = bucket_size_gb
            total_gcs_gb += bucket_size_gb
            
    except Exception as e:
        print(f"GCS 리소스 수집 오류: {e}")
    
    gcs_usage['total_gcs_gb'] = round(total_gcs_gb, 2)
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
        
        # 데이터 정리
        all_data = []
        
        # 헤더
        headers = ['Category', 'Instance/Resource', 'Zone', 'Machine Type', 'Status', 
          'CPU', 'Memory(GB)', 'Private IPs', 'Public IPs',
          'PD-Standard(GB)', 'PD-Balanced(GB)', 'PD-SSD(GB)', 'Local-SSD(GB)']
        all_data.append(headers)
        
        # 프로젝트 정보
        all_data.append(['Project Info', result_data['project_id'], '', '', '', '', '', '', '', '', ''])
        all_data.append(['Collection Time', result_data['timestamp'], '', '', '', '', '', '', '', '', ''])
        all_data.append(['', '', '', '', '', '', '', '', '', '', ''])  # 빈 줄
        
        # 인스턴스별 상세 정보
        for instance in result_data['instances']:
            row = [
                'Instance',
                instance.get('name', 'unknown'),
                instance.get('zone', ''),
                instance.get('machine_type', ''),
                instance.get('status', ''),
                instance.get('cpu', 0),
                round(float(instance.get('memory_gb', 0)), 2),
                instance.get('private_ips', 'None'),
                instance.get('public_ips', 'None'),
                instance.get('disks', {}).get('pd-standard', 0),
                instance.get('disks', {}).get('pd-balanced', 0),
                instance.get('disks', {}).get('pd-ssd', 0),
                instance.get('disks', {}).get('local-ssd', 0)
            ]
            all_data.append(row)
        
        all_data.append(['', '', '', '', '', '', '', '', '', '', ''])  # 빈 줄
        
        # 스냅샷
        all_data.append(['Snapshot', 'Total Snapshots', '', '', '', '', 
                        result_data['snapshot_total_gb'], '', '', '', ''])
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
        import traceback
        traceback.print_exc()
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
        print(f"수집된 인스턴스 수: {len(instances)}")
        
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
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    main()