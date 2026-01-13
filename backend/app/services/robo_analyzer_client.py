"""Robo Analyzer API Client.

OLAP에서 DW 테이블을 Neo4j에 등록할 때 robo-analyzer API를 경유하여
벡터 임베딩까지 함께 생성되도록 합니다.
"""
import logging
import httpx
from typing import Dict, List, Optional
from dataclasses import dataclass, asdict

from ..core.config import get_settings

logger = logging.getLogger(__name__)


@dataclass
class DWColumnInfo:
    """DW 컬럼 정보"""
    name: str
    dtype: str = "VARCHAR"
    description: Optional[str] = None
    is_pk: bool = False
    is_fk: bool = False
    fk_target_table: Optional[str] = None


@dataclass
class DWDimensionInfo:
    """DW 디멘전 테이블 정보"""
    name: str
    columns: List[DWColumnInfo] = None
    source_tables: List[str] = None
    
    def __post_init__(self):
        self.columns = self.columns or []
        self.source_tables = self.source_tables or []


@dataclass
class DWFactTableInfo:
    """DW 팩트 테이블 정보"""
    name: str
    columns: List[DWColumnInfo] = None
    source_tables: List[str] = None
    
    def __post_init__(self):
        self.columns = self.columns or []
        self.source_tables = self.source_tables or []


class RoboAnalyzerClient:
    """Robo Analyzer API 클라이언트
    
    DW 테이블을 Neo4j에 등록할 때 robo-analyzer의 벡터라이징 기능을 활용합니다.
    """
    
    def __init__(self, base_url: Optional[str] = None, api_key: Optional[str] = None):
        settings = get_settings()
        self.base_url = base_url or settings.robo_analyzer_url
        self.api_key = api_key or settings.openai_api_key
        self.timeout = 60.0
    
    async def register_star_schema(
        self,
        cube_name: str,
        fact_table: DWFactTableInfo,
        dimensions: List[DWDimensionInfo],
        db_name: str = "postgres",
        dw_schema: str = "dw",
        create_embeddings: bool = True
    ) -> Dict:
        """DW 스타스키마를 robo-analyzer를 통해 Neo4j에 등록
        
        Args:
            cube_name: 큐브 이름
            fact_table: 팩트 테이블 정보
            dimensions: 디멘전 테이블 목록
            db_name: 데이터베이스 이름
            dw_schema: DW 스키마명
            create_embeddings: 임베딩 생성 여부
            
        Returns:
            등록 결과 (success, tables_created, columns_created, embeddings_created)
        """
        url = f"{self.base_url}/robo/schema/dw-tables"
        
        # Dataclass를 dict로 변환
        fact_dict = {
            "name": fact_table.name,
            "columns": [asdict(c) if isinstance(c, DWColumnInfo) else c for c in fact_table.columns],
            "source_tables": fact_table.source_tables
        }
        
        dims_list = []
        for dim in dimensions:
            dim_dict = {
                "name": dim.name,
                "columns": [asdict(c) if isinstance(c, DWColumnInfo) else c for c in dim.columns],
                "source_tables": dim.source_tables
            }
            dims_list.append(dim_dict)
        
        payload = {
            "cube_name": cube_name,
            "db_name": db_name,
            "dw_schema": dw_schema,
            "fact_table": fact_dict,
            "dimensions": dims_list,
            "create_embeddings": create_embeddings
        }
        
        headers = {
            "Content-Type": "application/json",
            "X-API-Key": self.api_key
        }
        
        logger.info("[RoboAnalyzerClient] DW 스타스키마 등록 요청 | cube=%s | url=%s", cube_name, url)
        
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.post(url, json=payload, headers=headers)
                
                if response.status_code == 200:
                    result = response.json()
                    logger.info(
                        "[RoboAnalyzerClient] DW 스타스키마 등록 성공 | tables=%s | columns=%s | embeddings=%s",
                        result.get("tables_created", 0),
                        result.get("columns_created", 0),
                        result.get("embeddings_created", 0)
                    )
                    return result
                else:
                    error_msg = response.text
                    logger.error("[RoboAnalyzerClient] DW 스타스키마 등록 실패 | status=%d | error=%s", response.status_code, error_msg)
                    return {
                        "success": False,
                        "error": f"HTTP {response.status_code}: {error_msg}"
                    }
        except httpx.ConnectError as e:
            logger.warning("[RoboAnalyzerClient] robo-analyzer 연결 실패, 직접 Neo4j 등록으로 폴백 | error=%s", e)
            return {
                "success": False,
                "error": f"Connection failed: {e}",
                "fallback_required": True
            }
        except Exception as e:
            logger.error("[RoboAnalyzerClient] 예외 발생 | error=%s", e)
            return {
                "success": False,
                "error": str(e)
            }
    
    async def delete_star_schema(
        self,
        cube_name: str,
        dw_schema: str = "dw",
        db_name: str = "postgres"
    ) -> Dict:
        """DW 스타스키마를 Neo4j에서 삭제
        
        Args:
            cube_name: 삭제할 큐브 이름
            dw_schema: DW 스키마명
            db_name: 데이터베이스 이름
        """
        url = f"{self.base_url}/robo/schema/dw-tables/{cube_name}"
        params = {"dw_schema": dw_schema, "db_name": db_name}
        
        logger.info("[RoboAnalyzerClient] DW 스타스키마 삭제 요청 | cube=%s", cube_name)
        
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.delete(url, params=params)
                
                if response.status_code == 200:
                    result = response.json()
                    logger.info("[RoboAnalyzerClient] DW 스타스키마 삭제 성공 | cube=%s", cube_name)
                    return result
                else:
                    error_msg = response.text
                    logger.error("[RoboAnalyzerClient] DW 스타스키마 삭제 실패 | status=%d", response.status_code)
                    return {
                        "success": False,
                        "error": f"HTTP {response.status_code}: {error_msg}"
                    }
        except Exception as e:
            logger.error("[RoboAnalyzerClient] 삭제 예외 발생 | error=%s", e)
            return {
                "success": False,
                "error": str(e)
            }
    
    async def vectorize_schema(
        self,
        schema: str = "dw",
        db_name: str = "postgres",
        include_tables: bool = True,
        include_columns: bool = True,
        reembed_existing: bool = False
    ) -> Dict:
        """기존 DW 테이블/컬럼 벡터라이징
        
        이미 Neo4j에 등록된 테이블/컬럼에 대해 벡터 임베딩을 생성합니다.
        """
        url = f"{self.base_url}/robo/schema/vectorize"
        
        payload = {
            "db_name": db_name,
            "schema": schema,
            "include_tables": include_tables,
            "include_columns": include_columns,
            "reembed_existing": reembed_existing
        }
        
        headers = {
            "Content-Type": "application/json",
            "X-API-Key": self.api_key
        }
        
        logger.info("[RoboAnalyzerClient] 벡터라이징 요청 | schema=%s", schema)
        
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.post(url, json=payload, headers=headers)
                
                if response.status_code == 200:
                    result = response.json()
                    logger.info(
                        "[RoboAnalyzerClient] 벡터라이징 성공 | tables=%s | columns=%s",
                        result.get("tables_vectorized", 0),
                        result.get("columns_vectorized", 0)
                    )
                    return result
                else:
                    return {
                        "success": False,
                        "error": f"HTTP {response.status_code}"
                    }
        except Exception as e:
            logger.error("[RoboAnalyzerClient] 벡터라이징 예외 발생 | error=%s", e)
            return {
                "success": False,
                "error": str(e)
            }


# 싱글톤 인스턴스
robo_analyzer_client = RoboAnalyzerClient()
