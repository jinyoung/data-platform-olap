"""ETL 단위 테스트"""
import asyncio
import json

# Test 1: ETL Config 생성 테스트
async def test_create_etl_config():
    print("=" * 50)
    print("Test 1: ETL Config 생성")
    print("=" * 50)
    
    from app.services.etl_service import etl_service
    
    # 테스트 설정 생성
    config = await etl_service.create_etl_config(
        cube_name="정수장별유량",
        fact_table="dw.fact_flow",
        dimension_tables=["dw.dim_time", "dw.dim_site", "dw.dim_tag"],
        source_tables=["rwis.rdf01hh_tb", "rwis.rdisaup_tb", "rwis.rditag_tb"],
        mappings=[
            {"source_table": "rwis.rdf01hh_tb", "source_column": "log_time", "target_table": "fact_flow", "target_column": "log_time", "transformation": ""},
            {"source_table": "rwis.rdf01hh_tb", "source_column": "tagsn", "target_table": "fact_flow", "target_column": "tagsn", "transformation": ""},
            {"source_table": "rwis.rdf01hh_tb", "source_column": "val", "target_table": "fact_flow", "target_column": "flow_value", "transformation": "AVG(val)"},
        ],
        dw_schema="dw",
        sync_mode="incremental",
        incremental_column="log_time"
    )
    
    print(f"✅ ETL Config 생성 성공: {config.cube_name}")
    print(f"   - Fact Table: {config.fact_table}")
    print(f"   - Dimensions: {config.dimension_tables}")
    print(f"   - Mappings: {len(config.mappings)}개")
    return True

# Test 2: ETL Config 조회 테스트
async def test_get_etl_config():
    print("\n" + "=" * 50)
    print("Test 2: ETL Config 조회")
    print("=" * 50)
    
    from app.services.etl_service import etl_service
    
    config = etl_service.get_etl_config("정수장별유량")
    
    if config:
        print(f"✅ ETL Config 조회 성공: {config.cube_name}")
        print(f"   - Sync Mode: {config.sync_mode}")
        print(f"   - Created At: {config.created_at}")
        return True
    else:
        print("❌ ETL Config를 찾을 수 없습니다")
        return False

# Test 3: 파일 저장 확인
async def test_file_persistence():
    print("\n" + "=" * 50)
    print("Test 3: 파일 저장 확인")
    print("=" * 50)
    
    from pathlib import Path
    config_file = Path("data/etl_configs.json")
    
    if config_file.exists():
        with open(config_file, 'r') as f:
            data = json.load(f)
        
        if "정수장별유량" in data:
            print(f"✅ 파일에 ETL Config 저장됨")
            print(f"   - 저장된 큐브: {list(data.keys())}")
            return True
        else:
            print("❌ 파일에 ETL Config가 없습니다")
            return False
    else:
        print("❌ ETL Config 파일이 없습니다")
        return False

# Test 4: 모든 ETL Config 조회
async def test_get_all_configs():
    print("\n" + "=" * 50)
    print("Test 4: 모든 ETL Config 조회")
    print("=" * 50)
    
    from app.services.etl_service import etl_service
    
    configs = etl_service.get_all_etl_configs()
    print(f"✅ 총 {len(configs)}개의 ETL Config")
    for name in configs:
        print(f"   - {name}")
    return True

# 메인 실행
async def main():
    print("\n🧪 ETL 단위 테스트 시작\n")
    
    results = []
    results.append(("ETL Config 생성", await test_create_etl_config()))
    results.append(("ETL Config 조회", await test_get_etl_config()))
    results.append(("파일 저장 확인", await test_file_persistence()))
    results.append(("모든 Config 조회", await test_get_all_configs()))
    
    print("\n" + "=" * 50)
    print("테스트 결과 요약")
    print("=" * 50)
    
    passed = sum(1 for _, r in results if r)
    failed = sum(1 for _, r in results if not r)
    
    for name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"  {status}: {name}")
    
    print(f"\n총 {len(results)}개 테스트: {passed} 성공, {failed} 실패")

if __name__ == "__main__":
    asyncio.run(main())
