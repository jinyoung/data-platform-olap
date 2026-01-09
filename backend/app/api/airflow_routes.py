"""Airflow API Routes - Manage ETL pipelines via Apache Airflow."""
from typing import Optional
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from ..services.airflow_service import airflow_service
from ..services.etl_service import etl_service

router = APIRouter(prefix="/airflow", tags=["Airflow"])


class GenerateDAGRequest(BaseModel):
    """Request to generate a DAG from ETL config."""
    cube_name: str


class TriggerDAGRequest(BaseModel):
    """Request to trigger a DAG run."""
    dag_id: str


# ============== DAG Generation ==============

@router.post("/dag/generate")
async def generate_dag(request: GenerateDAGRequest):
    """Generate Airflow DAG from ETL configuration.
    
    This creates a Python DAG file in the Airflow dags folder
    that can be executed by Airflow scheduler.
    """
    # Get ETL config for the cube
    config = etl_service.get_etl_config(request.cube_name)
    
    if not config:
        raise HTTPException(
            status_code=404, 
            detail=f"No ETL config found for cube: {request.cube_name}"
        )
    
    try:
        # Generate and save DAG
        dag_info = airflow_service.save_dag(config.to_dict())
        
        return {
            "success": True,
            "dag_id": dag_info.dag_id,
            "file_path": dag_info.file_path,
            "airflow_url": dag_info.airflow_url,
            "message": f"DAG '{dag_info.dag_id}' created successfully. "
                       f"View at: {dag_info.airflow_url}"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to generate DAG: {str(e)}")


@router.post("/dag/generate-from-config")
async def generate_dag_from_config(etl_config: dict):
    """Generate Airflow DAG directly from ETL config dict.
    
    Use this when you want to generate a DAG without saving ETL config first.
    """
    try:
        dag_info = airflow_service.save_dag(etl_config)
        
        return {
            "success": True,
            "dag_id": dag_info.dag_id,
            "file_path": dag_info.file_path,
            "airflow_url": dag_info.airflow_url
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to generate DAG: {str(e)}")


# ============== DAG Execution ==============

@router.post("/dag/{dag_id}/trigger")
async def trigger_dag(dag_id: str):
    """Trigger a DAG run.
    
    This will start the ETL pipeline in Airflow.
    """
    result = await airflow_service.trigger_dag(dag_id)
    
    if result.get("success"):
        return result
    else:
        raise HTTPException(
            status_code=500, 
            detail=f"Failed to trigger DAG: {result.get('error')}"
        )


@router.get("/dag/{dag_id}/status")
async def get_dag_status(dag_id: str):
    """Get DAG status and recent runs."""
    result = await airflow_service.get_dag_status(dag_id)
    
    if result.get("success"):
        return result
    else:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get DAG status: {result.get('error')}"
        )


# ============== DAG Management ==============

@router.get("/dags")
async def list_dags():
    """List all generated ETL DAGs."""
    dags = airflow_service.list_dags()
    return {
        "dags": dags,
        "total": len(dags)
    }


@router.delete("/dag/{dag_id}")
async def delete_dag(dag_id: str):
    """Delete a DAG file."""
    success = airflow_service.delete_dag(dag_id)
    
    if success:
        return {"success": True, "message": f"DAG '{dag_id}' deleted"}
    else:
        raise HTTPException(status_code=404, detail=f"DAG '{dag_id}' not found")


# ============== Health Check ==============

@router.get("/health")
async def airflow_health():
    """Check Airflow connectivity."""
    result = await airflow_service.check_airflow_health()
    return result


# ============== Convenience Endpoints ==============

@router.post("/etl/{cube_name}/deploy")
async def deploy_etl_pipeline(cube_name: str):
    """Deploy ETL pipeline for a cube.
    
    This is a convenience endpoint that:
    1. Gets the ETL config for the cube
    2. Generates an Airflow DAG
    3. Returns the Airflow URL to monitor the pipeline
    """
    # Get ETL config
    config = etl_service.get_etl_config(cube_name)
    
    if not config:
        raise HTTPException(
            status_code=404,
            detail=f"No ETL config found for cube: {cube_name}"
        )
    
    try:
        # Generate DAG
        dag_info = airflow_service.save_dag(config.to_dict())
        
        return {
            "success": True,
            "cube_name": cube_name,
            "dag_id": dag_info.dag_id,
            "airflow_url": dag_info.airflow_url,
            "message": f"ETL pipeline deployed. Open Airflow to trigger: {dag_info.airflow_url}"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/etl/{cube_name}/run")
async def run_etl_pipeline(cube_name: str):
    """Deploy and trigger ETL pipeline for a cube.
    
    This:
    1. Generates the DAG (if not exists)
    2. Triggers the DAG run
    3. Returns status and monitoring URL
    """
    # Get ETL config
    config = etl_service.get_etl_config(cube_name)
    
    if not config:
        raise HTTPException(
            status_code=404,
            detail=f"No ETL config found for cube: {cube_name}"
        )
    
    try:
        # Generate DAG
        dag_info = airflow_service.save_dag(config.to_dict())
        
        # Trigger DAG
        trigger_result = await airflow_service.trigger_dag(dag_info.dag_id)
        
        if trigger_result.get("success"):
            return {
                "success": True,
                "cube_name": cube_name,
                "dag_id": dag_info.dag_id,
                "dag_run_id": trigger_result.get("dag_run_id"),
                "state": trigger_result.get("state"),
                "airflow_url": dag_info.airflow_url,
                "message": f"ETL pipeline triggered. Monitor at: {dag_info.airflow_url}"
            }
        else:
            return {
                "success": False,
                "cube_name": cube_name,
                "dag_id": dag_info.dag_id,
                "airflow_url": dag_info.airflow_url,
                "error": trigger_result.get("error"),
                "message": f"DAG created but trigger failed. Check Airflow: {dag_info.airflow_url}"
            }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

