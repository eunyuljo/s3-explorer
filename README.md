# s3-explorer

---

■ 환경

	1. AL 2023 ( 실행 환경 cloudshell 사용 )
	
	2. python3
	
	2-1. pip 
    	sudo dnf install python3-pip -y

	4. python3 패키지
    	python3 -m pip install tabulate boto3

	5. AWS IAM 및 AWSCLI Credetial profile 세팅
	    $ aws configure --profile eyjo


---


■ 실행 방법

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