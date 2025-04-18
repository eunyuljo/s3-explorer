import boto3
from botocore.exceptions import ClientError
from tabulate import tabulate
from collections import defaultdict
from datetime import datetime
import csv
import sys
import concurrent.futures
import time
import argparse

# AWS CLI 프로필 기본값 설정
DEFAULT_PROFILE = None  # 기본값은 None으로 설정하여 AWS CLI의 기본 프로필 또는 환경 변수 사용

# 스토리지 클래스 별 요율 (GB 기준, 예시 값)
storage_class_rates = {
    "STANDARD": 0.025,
    "STANDARD_IA": 0.0138,
    "ONEZONE_IA": 0.01,
    "GLACIER": 0.0045,
    "DEEP_ARCHIVE": 0.002,
    "INTELLIGENT_TIERING": 0.023,
    "REDUCED_REDUNDANCY": 0.023,
    "GLACIER_IR": 0.005
}
# 대량 사용 할인 적용을 위한 단계별 요율
def calculate_discounted_cost(total_gb):
    if total_gb <= 50 * 1024:
        return total_gb * 0.023  # 0 ~ 50 TB
    elif total_gb <= 450 * 1024:
        return total_gb * 0.0225  # 50 ~ 450 TB
    elif total_gb <= 500 * 1024:
        return total_gb * 0.0215  # 450 ~ 500 TB
    else:
        return total_gb * 0.02  # 500 TB 이상

def analyze_bucket(bucket_name, creation_date, session, fast_mode=False):
    """단일 버킷 분석 함수"""
    s3 = session.client('s3')
    start_time = time.time()
    print(f"🔍 분석 시작: {bucket_name}")
    
    storage_class_size = defaultdict(int)
    
    # 리전 가져오기
    try:
        region = s3.get_bucket_location(Bucket=bucket_name)['LocationConstraint'] or 'us-east-1'
    except Exception as e:
        region = 'Unknown'
        print(f"⚠️ 리전 확인 오류 {bucket_name}: {e}")
        
    # 태그 검색 - S3 버킷 태그가 있는 경우 가져오기
    try:
        tags = s3.get_bucket_tagging(Bucket=bucket_name)
        tags_dict = {tag['Key']: tag['Value'] for tag in tags.get('TagSet', [])}
    except Exception:
        tags_dict = {}
    
    # S3 인벤토리 설정이 있는지 확인 (나중에 활용 가능)
    has_inventory = False
    try:
        inventory_configs = s3.list_bucket_inventory_configurations(Bucket=bucket_name)
        if 'InventoryConfigurationList' in inventory_configs and inventory_configs['InventoryConfigurationList']:
            has_inventory = True
    except Exception:
        pass
    
    # CloudWatch 메트릭을 사용하여 모든 스토리지 클래스의 크기 추정 시도 (더 빠른 방법)
    try:
        cloudwatch = session.client('cloudwatch')
        now = datetime.utcnow()
        
        # 모든 스토리지 클래스 유형 정의
        storage_types = [
            'StandardStorage',           # 표준
            'StandardIAStorage',         # 표준-IA
            'StandardOneZoneIAStorage',  # 단일 영역-IA
            'ReducedRedundancyStorage',  # RRS (레거시)
            'GlacierStorage',            # Glacier
            'GlacierIRStorage',          # Glacier IR
            'GlacierDeepArchiveStorage', # Glacier Deep Archive
            'IntelligentTieringFAStorage', # Intelligent-Tiering
            'IntelligentTieringIAStorage', # Intelligent-Tiering IA
            'IntelligentTieringAAStorage', # Intelligent-Tiering Archive Access
            'IntelligentTieringDAAStorage', # Intelligent-Tiering Deep Archive Access
        ]
        
        # 스토리지 클래스 매핑 (CloudWatch 타입 -> S3 API 타입)
        storage_class_mapping = {
            'StandardStorage': 'STANDARD',
            'StandardIAStorage': 'STANDARD_IA',
            'StandardOneZoneIAStorage': 'ONEZONE_IA',
            'ReducedRedundancyStorage': 'REDUCED_REDUNDANCY',
            'GlacierStorage': 'GLACIER',
            'GlacierIRStorage': 'GLACIER_IR',
            'GlacierDeepArchiveStorage': 'DEEP_ARCHIVE',
            'IntelligentTieringFAStorage': 'INTELLIGENT_TIERING',
            'IntelligentTieringIAStorage': 'INTELLIGENT_TIERING',
            'IntelligentTieringAAStorage': 'INTELLIGENT_TIERING',
            'IntelligentTieringDAAStorage': 'INTELLIGENT_TIERING'
        }
        
        has_metrics = False
        
        # 각 스토리지 클래스별로 메트릭 조회
        for storage_type in storage_types:
            response = cloudwatch.get_metric_statistics(
                Namespace='AWS/S3',
                MetricName='BucketSizeBytes',
                Dimensions=[
                    {'Name': 'BucketName', 'Value': bucket_name},
                    {'Name': 'StorageType', 'Value': storage_type}
                ],
                StartTime=now.replace(day=1, hour=0, minute=0, second=0, microsecond=0),
                EndTime=now,
                Period=86400,
                Statistics=['Average'],
            )
            
            if response['Datapoints']:
                has_metrics = True
                class_size = max([point['Average'] for point in response['Datapoints']])
                
                # 해당 스토리지 클래스에 크기 할당
                s3_storage_class = storage_class_mapping.get(storage_type, 'STANDARD')
                storage_class_size[s3_storage_class] += class_size
                print(f"  - {storage_type}: {class_size / (1024**3):.2f} GB")
        
        if has_metrics:
            print(f"✅ {bucket_name}: 모든 스토리지 클래스 메트릭 사용 완료 ({time.time() - start_time:.2f}초)")
        elif not fast_mode:
            # 메트릭이 없으면 객체 나열 방식으로 대체
            paginator = s3.get_paginator('list_objects_v2')
            
            # 최적화: 첫 페이지만 가져와서 객체 총 개수 확인
            first_page = next(paginator.paginate(Bucket=bucket_name, MaxItems=1000), None)
            if first_page and first_page.get('KeyCount', 0) > 10000:
                # 객체가 많으면 HEAD 요청으로 버킷 크기만 확인 (빠른 추정)
                print(f"⚡ {bucket_name}에 객체가 많음: 빠른 추정 모드로 전환")
                response = s3.head_bucket(Bucket=bucket_name)
                
                # 샘플링으로 추정 - 스토리지 클래스별 분포 파악
                storage_classes_count = defaultdict(int)
                total_sampled = 0
                sampled_size = 0
                
                # 샘플 데이터 수집 (처음 1000개 객체)
                for page in paginator.paginate(Bucket=bucket_name, PaginationConfig={'MaxItems': 1000}):
                    for obj in page.get('Contents', []):
                        size = obj['Size']
                        storage_class = obj.get('StorageClass', 'STANDARD')
                        storage_class_size[storage_class] += size
                        storage_classes_count[storage_class] += 1
                        sampled_size += size
                        total_sampled += 1
                
                # 전체 객체 수 가져오기 (HEAD 요청으로 빠르게 확인)
                total_estimated_count = first_page.get('KeyCount', 0)
                
                # 스토리지 클래스별 분포 기반 전체 크기 추정
                if total_sampled > 0:
                    # 각 스토리지 클래스별 비율 계산 및 총 크기 추정
                    for storage_class, count in storage_classes_count.items():
                        ratio = count / total_sampled
                        estimated_size = (sampled_size / total_sampled) * total_estimated_count * ratio
                        storage_class_size[storage_class] = estimated_size
                    print(f"  - 샘플링: {total_sampled}개 객체 분석으로 {total_estimated_count}개 추정")
                
                print(f"✅ {bucket_name}: 샘플링 기반 추정 완료 ({time.time() - start_time:.2f}초)")
            elif first_page:
                # 객체가 적으면 정상적으로 전체 나열
                for page in paginator.paginate(Bucket=bucket_name):
                    for obj in page.get('Contents', []):
                        size = obj['Size']
                        storage_class = obj.get('StorageClass', 'STANDARD')
                        storage_class_size[storage_class] += size

                print(f"✅ {bucket_name}: 전체 객체 분석 완료 ({time.time() - start_time:.2f}초)")
            else:
                # 접근 권한이 없거나 비어있는 버킷
                print(f"⚠️ {bucket_name}: 비어있거나 접근 권한이 없는 버킷")
    except Exception as e:
        print(f"⚠️ {bucket_name} 분석 오류: {e}")

    # 수명주기 정책 확인
    try:
        lifecycle = s3.get_bucket_lifecycle_configuration(Bucket=bucket_name)
        rules = lifecycle.get('Rules', [])
        if rules:
            rule_summary = f"{len(rules)} rules"
            # 단수형과 복수형 모두 확인
            has_transition = any('Transition' in rule or 'Transitions' in rule for rule in rules)
            has_expiration = any('Expiration' in rule or 'Expirations' in rule for rule in rules)
            rule_summary += f", T:{has_transition}, E:{has_expiration}"
            lifecycle_policy = rule_summary
        else:
            lifecycle_policy = "NO"
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchLifecycleConfiguration':
            lifecycle_policy = "NO"
        else:
            lifecycle_policy = "Error"

    # 총 용량 및 비용 계산
    total_size = sum(storage_class_size.values())
    total_gb = total_size / (1024 ** 3)

    # 대량 사용 할인 적용
    estimated_cost = calculate_discounted_cost(total_gb)
    
    # 결과 반환
    return {
        'bucket_name': bucket_name,
        'creation_date': creation_date,
        'region': region,
        'total_gb': total_gb,
        'lifecycle_policy': lifecycle_policy,
        'estimated_cost': estimated_cost,
        'storage_class_size': dict(storage_class_size),
        'tags': tags_dict
    }

def main():
    start_time = time.time()
    print("📦 S3 Bucket Size Check 시작...\n")
    
    # 명령줄 인자 파싱
    parser = argparse.ArgumentParser(description='S3 버킷 용량 분석 및 비용 추정 도구')
    parser.add_argument('-w', '--workers', type=int, default=10, 
                      help='동시 처리할 최대 스레드 수 (기본값: 10)')
    parser.add_argument('-f', '--fast', action='store_true',
                      help='고속 모드: CloudWatch 메트릭만 사용하고 객체 나열 건너뛰기')
    parser.add_argument('-r', '--region', type=str,
                      help='특정 리전의 버킷만 분석')
    parser.add_argument('-n', '--name', type=str,
                      help='이름에 특정 문자열을 포함하는 버킷만 분석')
    parser.add_argument('-t', '--top', type=int,
                      help='크기가 가장 큰 N개의 버킷만 분석 (결과 정렬 후)')
    parser.add_argument('-p', '--profile', type=str, default=DEFAULT_PROFILE,
                      help='사용할 AWS 프로필 (기본값: 기본 AWS 프로필)')
    
    args = parser.parse_args()
    
    # 프로필 설정
    aws_profile_to_use = args.profile
    
    # boto3 세션 생성
    session = boto3.Session(profile_name=aws_profile_to_use)
    s3 = session.client('s3')
    
    # 모든 버킷 리스트 가져오기
    buckets = s3.list_buckets()['Buckets']
    
    # 필터링 적용
    if args.name:
        print(f"필터: '{args.name}'을 포함하는 버킷만 분석")
        filtered_buckets = [b for b in buckets if args.name in b['Name']]
    else:
        filtered_buckets = buckets
    
    if args.region:
        print(f"필터: '{args.region}' 리전의 버킷만 분석")
        region_filtered = []
        for bucket in filtered_buckets:
            try:
                region = s3.get_bucket_location(Bucket=bucket['Name'])['LocationConstraint'] or 'us-east-1'
                if region == args.region:
                    region_filtered.append(bucket)
            except Exception:
                pass
        filtered_buckets = region_filtered
    
    bucket_count = len(filtered_buckets)
    print(f"총 {bucket_count}개의 버킷 분석 대상")
    
    # 병렬 처리를 위한 작업 목록 준비
    bucket_data = []
    
    # ThreadPoolExecutor로 병렬 처리
    max_workers = min(args.workers, bucket_count)
    print(f"병렬 처리 시작 (최대 {max_workers}개 동시 처리)")
    
    # 고속 모드 알림
    if args.fast:
        print("⚡ 고속 모드 활성화: CloudWatch 메트릭만 사용하여 빠르게 분석합니다")
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 각 버킷에 대한 작업 제출
        future_to_bucket = {
            executor.submit(analyze_bucket, bucket['Name'], bucket['CreationDate'].strftime("%Y-%m-%d"), session, args.fast): bucket['Name']
            for bucket in filtered_buckets
        }
        
        # 작업 완료 대기 및 결과 수집
        completed = 0
        for future in concurrent.futures.as_completed(future_to_bucket):
            bucket_name = future_to_bucket[future]
            try:
                result = future.result()
                
                # 버킷의 스토리지 클래스별 크기 정보
                storage_class_sizes = {}
                for storage_class, size in result['storage_class_size'].items():
                    size_gb = size / (1024 ** 3)
                    if size_gb >= 0.01:  # 0.01GB(10MB) 이상인 경우만 표시
                        storage_class_sizes[storage_class] = f"{size_gb:.2f} GB"
                
                # 버킷 데이터에 추가
                bucket_data.append([
                    result['bucket_name'],
                    result['creation_date'],
                    result['region'],
                    f"{result['total_gb']:,.2f} GB",
                    storage_class_sizes,  # 스토리지 클래스별 크기 정보
                    result['lifecycle_policy'],
                    f"${result['estimated_cost']:,.2f}"
                ])
                completed += 1
                print(f"진행 상황: {completed}/{bucket_count} ({completed/bucket_count*100:.1f}%)")
            except Exception as e:
                print(f"⚠️ {bucket_name} 처리 중 오류: {e}")
    
    # 버킷 크기 기준으로 내림차순 정렬
    bucket_data.sort(key=lambda x: float(x[3].split()[0].replace(',', '')), reverse=True)
    
    # 만약 top 옵션이 있다면 결과를 크기 기준으로 정렬 후 상위 N개만 선택
    if args.top and args.top < len(bucket_data):
        # 크기로 정렬 (이미 정렬되어 있지만 재확인)
        bucket_data.sort(key=lambda x: float(x[3].split()[0].replace(',', '')), reverse=True)
        print(f"\n상위 {args.top}개 큰 버킷만 표시합니다 (전체 {len(bucket_data)}개 중)")
        bucket_data = bucket_data[:args.top]
    
    # 스토리지 클래스별 요약 정보 추가
    storage_class_totals = defaultdict(float)
    for future in future_to_bucket:
        try:
            result = future.result()
            for storage_class, size in result['storage_class_size'].items():
                storage_class_totals[storage_class] += size / (1024 ** 3)  # GB로 변환
        except Exception:
            pass
    
    # 출력
    headers = [
        "Bucket Name", "Created", "Region", "Total Size (GB)", "Storage Classes", "Lifecycle Policy",
        "Estimated Cost (USD)"
    ]
    
    # 출력 형식 조정 - 스토리지 클래스 정보를 텍스트로 변환
    for row in bucket_data:
        storage_classes_info = row[4]  # 스토리지 클래스 정보 (딕셔너리)
        
        # 딕셔너리를 문자열로 변환
        if storage_classes_info:
            storage_classes_text = "\n".join([f"{cls}: {size}" for cls, size in storage_classes_info.items()])
        else:
            storage_classes_text = "N/A"
        
        # 딕셔너리를 문자열로 대체
        row[4] = storage_classes_text
    
    print("\n📊 === S3 Bucket Summary (Monthly) ===\n")
    print(tabulate(bucket_data, headers=headers, tablefmt="fancy_grid", colalign=["center", "center", "center", "right", "left", "center", "right"]))
    
    # 총합 계산 (인덱스 수정: 총 크기는 3번 인덱스, 비용은 6번 인덱스)
    total_size_gb = sum([float(row[3].split()[0].replace(',', '')) for row in bucket_data])
    total_cost = sum([float(row[6].replace('$', '').replace(',', '')) for row in bucket_data])
    print("-" * 80)
    print(f"{'TOTAL':<40} {total_size_gb:>15,.2f} GB {total_cost:>20,.2f} USD")
    
    # 스토리지 클래스별 요약 출력
    print("\n📊 === 스토리지 클래스별 요약 ===")
    storage_class_data = []
    for storage_class, total_gb in sorted(storage_class_totals.items(), key=lambda x: x[1], reverse=True):
        # 각 스토리지 클래스별 비용 계산
        rate = storage_class_rates.get(storage_class, storage_class_rates["STANDARD"])
        class_cost = total_gb * rate
        storage_class_data.append([
            storage_class,
            f"{total_gb:,.2f} GB",
            f"{(total_gb/total_size_gb*100) if total_size_gb else 0:.1f}%",
            f"${class_cost:,.2f}",
            f"${rate:.4f}/GB"
        ])
    
    print(tabulate(storage_class_data, 
                headers=["Storage Class", "Size (GB)", "% of Total", "Est. Cost (USD)", "Rate"],
                tablefmt="fancy_grid"))
    
    # CSV 저장용 데이터 준비 - 스토리지 클래스 정보를 CSV 호환 형식으로 변환
    csv_data = []
    for row in bucket_data:
        # 원본 데이터 복사
        csv_row = row.copy()
        
        # 스토리지 클래스 정보 포맷팅 (CSV에서는 줄바꿈이 문제될 수 있음)
        storage_classes = csv_row[4]
        if isinstance(storage_classes, str):  # 이미 문자열로 변환된 경우
            csv_row[4] = storage_classes.replace("\n", " | ")
        
        csv_data.append(csv_row)
    
    # CSV 저장
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f's3_report_{timestamp}.csv'
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(csv_data)
        
        # 스토리지 클래스 요약 정보도 추가
        writer.writerow([])
        writer.writerow(["Storage Class Summary"])
        writer.writerow(["Storage Class", "Size (GB)", "% of Total", "Est. Cost (USD)", "Rate"])
        writer.writerows(storage_class_data)
    
    print(f"\n✅ 리포트가 {filename} 로 저장되었습니다.")
    print(f"⏱️ 총 실행 시간: {time.time() - start_time:.2f}초")

if __name__ == "__main__":
    main()