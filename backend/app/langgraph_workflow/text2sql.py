"""LangGraph workflow for Text2SQL conversion."""
import re
from typing import TypedDict, Optional, List, Any, Dict
from langchain_openai import ChatOpenAI
from langchain_core.messages import SystemMessage, HumanMessage
from langgraph.graph import StateGraph, END

from ..core.config import get_settings
from ..services.metadata_store import metadata_store
from ..services.db_executor import db_executor


class Text2SQLState(TypedDict):
    """State for the Text2SQL workflow."""
    question: str
    cube_name: Optional[str]
    schema_description: str
    generated_sql: str
    validated_sql: str
    result: Optional[Dict[str, Any]]
    error: Optional[str]


class Text2SQLWorkflow:
    """LangGraph workflow for converting natural language to SQL."""
    
    FORBIDDEN_KEYWORDS = [
        "DROP", "DELETE", "UPDATE", "INSERT", "ALTER", "TRUNCATE",
        "CREATE", "GRANT", "REVOKE", "EXECUTE", "EXEC"
    ]
    
    def __init__(self):
        settings = get_settings()
        self.llm = ChatOpenAI(
            model=settings.openai_model,
            api_key=settings.openai_api_key,
            temperature=0
        )
        self.max_rows = settings.max_rows
    
    async def load_metadata(self, state: Text2SQLState) -> Text2SQLState:
        """Node #1: Load cube metadata for the query."""
        cube_name = state.get("cube_name")
        schema_desc = metadata_store.get_schema_description(cube_name)
        
        if not schema_desc:
            state["error"] = "No cube metadata loaded. Please upload a schema first."
            return state
        
        state["schema_description"] = schema_desc
        return state
    
    async def generate_prompt(self, state: Text2SQLState) -> Text2SQLState:
        """Node #2: Generate the prompt for LLM."""
        # Prompt is assembled in the LLM node directly
        return state
    
    async def generate_sql(self, state: Text2SQLState) -> Text2SQLState:
        """Node #3: Generate SQL using LLM."""
        if state.get("error"):
            return state
        
        system_prompt = f"""You are an expert SQL analyst. Generate a PostgreSQL SELECT query based on the user's question and the provided schema.

RULES:
1. Only generate SELECT queries - no modifications allowed
2. Use only the tables and columns defined in the schema
3. Always add LIMIT {self.max_rows} to prevent large result sets
4. Use appropriate JOINs based on the schema relationships
5. For aggregations, always include GROUP BY for non-aggregated columns
6. Return ONLY the SQL query, no explanations

SCHEMA:
{state['schema_description']}
"""

        human_prompt = f"""Question: {state['question']}

Generate the PostgreSQL query:"""

        try:
            response = await self.llm.ainvoke([
                SystemMessage(content=system_prompt),
                HumanMessage(content=human_prompt)
            ])
            
            sql = response.content.strip()
            # Clean up markdown code blocks if present
            sql = re.sub(r'^```sql\s*', '', sql)
            sql = re.sub(r'^```\s*', '', sql)
            sql = re.sub(r'\s*```$', '', sql)
            
            state["generated_sql"] = sql.strip()
        except Exception as e:
            state["error"] = f"LLM error: {str(e)}"
        
        return state
    
    async def validate_sql(self, state: Text2SQLState) -> Text2SQLState:
        """Node #4: Validate the generated SQL."""
        if state.get("error"):
            return state
        
        sql = state.get("generated_sql", "")
        
        # Check for forbidden keywords
        sql_upper = sql.upper()
        for keyword in self.FORBIDDEN_KEYWORDS:
            # Check for keyword as a word (not part of another word)
            pattern = r'\b' + keyword + r'\b'
            if re.search(pattern, sql_upper):
                state["error"] = f"Forbidden SQL operation: {keyword}"
                return state
        
        # Ensure SELECT statement
        if not sql_upper.strip().startswith("SELECT"):
            state["error"] = "Only SELECT queries are allowed"
            return state
        
        # Ensure LIMIT is present, add if missing
        if "LIMIT" not in sql_upper:
            sql = sql.rstrip(";") + f" LIMIT {self.max_rows}"
        
        state["validated_sql"] = sql
        return state
    
    async def execute_query(self, state: Text2SQLState) -> Text2SQLState:
        """Node #5: Execute the validated SQL."""
        if state.get("error"):
            return state
        
        sql = state.get("validated_sql", "")
        if not sql:
            state["error"] = "No valid SQL to execute"
            return state
        
        result = await db_executor.execute_query(sql)
        
        if result.error:
            state["error"] = result.error
        else:
            state["result"] = {
                "sql": result.sql,
                "columns": result.columns,
                "rows": result.rows,
                "row_count": result.row_count,
                "execution_time_ms": result.execution_time_ms
            }
        
        return state
    
    def should_continue(self, state: Text2SQLState) -> str:
        """Determine if workflow should continue or end."""
        if state.get("error"):
            return "error"
        return "continue"


def create_text2sql_workflow() -> StateGraph:
    """Create and return the Text2SQL workflow graph."""
    workflow = Text2SQLWorkflow()
    
    # Create the graph
    graph = StateGraph(Text2SQLState)
    
    # Add nodes
    graph.add_node("load_metadata", workflow.load_metadata)
    graph.add_node("generate_sql", workflow.generate_sql)
    graph.add_node("validate_sql", workflow.validate_sql)
    graph.add_node("execute_query", workflow.execute_query)
    
    # Set entry point
    graph.set_entry_point("load_metadata")
    
    # Add conditional edges
    graph.add_conditional_edges(
        "load_metadata",
        workflow.should_continue,
        {
            "continue": "generate_sql",
            "error": END
        }
    )
    
    graph.add_conditional_edges(
        "generate_sql",
        workflow.should_continue,
        {
            "continue": "validate_sql",
            "error": END
        }
    )
    
    graph.add_conditional_edges(
        "validate_sql",
        workflow.should_continue,
        {
            "continue": "execute_query",
            "error": END
        }
    )
    
    graph.add_edge("execute_query", END)
    
    return graph.compile()


# Create a singleton workflow instance
_workflow = None

def get_workflow():
    """Get the compiled workflow instance."""
    global _workflow
    if _workflow is None:
        _workflow = create_text2sql_workflow()
    return _workflow

