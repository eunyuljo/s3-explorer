# s3-explorer python3 스크립트

## 스크립트 설명

    1. 용량 계산 및 비용을 조사하기 위한 스크립트
    2. 월별 예상 비용을 계산하여 출력하며 용량 크기를 기준으로 정렬함
    3. 수명 주기 정책 등의 여부를 조사하여 용량 기준으로 적용이 필요한 대상 집중 확인
    4. 대용량 버킷 분석을 위해 일반적인 모든 객체를 조사하는 것이 아닌 병렬처리를 처리
        Cloudwatch 메트릭을 통해 BucketSizeBytes 메트릭을 조회함
    5. 스토리지 클래스를 구분하여, 수명주기 사이클을 활용하여 적절한 유형을 사용중인지 확인
    
---

## 환경

	1. AL 2023 ( 실행 환경 cloudshell 사용 )
	
	2. python3
	
	2-1. pip 
    	sudo dnf install python3-pip -y

	3. python3 패키지
    	python3 -m pip install tabulate boto3

	4. AWS IAM 및 AWSCLI Credetial profile 세팅 ( readonlyaccess 혹은 s3, cloudwatch 에만 최소 정책 ) 
	    $ aws configure --profile eyjo


---


## 실행 방법

```
# 고속 모드 
python3 s3-explorer-new.py --fast 

# 프로필 지정 
python3 s3-explorer-new.py --profile your-profile 

# 처리 쓰레드 수 조정 
python3 s3-explorer-new.py --workers 5 

# 특정 리전 분석 
python3 s3-explorer-new.py --region ap-northeast-2
```

---

