from fastapi import APIRouter, HTTPException
from sqlalchemy import text
from pydantic import BaseModel
from database.connection import Connection
from typing import Annotated
from sqlalchemy.orm import Session
from fastapi import Depends

db_connection = Connection(db_hostname="mssql")
db_connection.create_tables()

class SQLQuery(BaseModel):
    query: str

class WorkbenchAPI:
    def __init__(self):
        self.router = APIRouter(prefix="/api/workbench", tags=["Workbench"])
        self.router.add_api_route("/schema", self.schema, methods=["GET"])
        self.router.add_api_route("/query", self.query, methods=["POST"])

    async def schema(self):
        try:
            with open('database/schema.sql', 'r') as file:
                sql_script = file.read()
            return {"data": sql_script}
        except FileNotFoundError:
            raise HTTPException(status_code=404, detail="Schema file not found")

    def query(self, sql: SQLQuery, db: Annotated[Session, Depends(db_connection.get_session_fastapi)]):
        query_text = sql.query.strip()
        if not query_text:
            raise HTTPException(status_code=400, detail="Missing 'query' in request body")
        
        try:
            result = db.execute(text(query_text))
            
            query_lower = query_text.lower()
            
            read_only_queries = ["select", "with", "show", "desc", "describe", "explain"]
            is_read_only = any(query_lower.startswith(cmd) for cmd in read_only_queries)
            
            # commit
            modification_queries = ["insert", "update", "delete", "merge", "truncate", 
                                "create", "alter", "drop", "exec", "execute", "sp_"]
            needs_commit = any(query_lower.startswith(cmd) for cmd in modification_queries)
            
            data = []
            
            if is_read_only:
                try:
                    rows = result.fetchall()
                    if rows:
                        columns = list(result.keys())
                        data = []
                        for row in rows:
                            row_dict = {}
                            for i, column in enumerate(columns):
                                value = row[i] if i < len(row) else None
                                # Convert datetime objects untuk JSON serialization
                                if hasattr(value, 'isoformat'):
                                    row_dict[column] = value.isoformat()
                                else:
                                    row_dict[column] = value
                            data.append(row_dict)
                except Exception as e:
                    data = [{"message": "Query executed successfully, no result set returned"}]
            
            elif needs_commit:
                try:
                    db.commit()
                    affected_rows = getattr(result, 'rowcount', 0)
                    
                    try:
                        rows = result.fetchall()
                        if rows:
                            columns = list(result.keys())
                            data = [dict(zip(columns, row)) for row in rows]
                        else:
                            data = [{"rows_affected": affected_rows}]
                    except:
                        data = [{"rows_affected": affected_rows}]
                        
                except Exception as commit_error:
                    db.rollback()
                    raise HTTPException(status_code=400, detail=f"Commit failed: {str(commit_error)}")
            
            else:
                # (DECLARE, SET, etc..)
                try:
                    # fetch
                    rows = result.fetchall()
                    if rows:
                        columns = list(result.keys())
                        data = [dict(zip(columns, row)) for row in rows]
                    else:
                        data = [{"message": "Query executed successfully"}]
                except:
                    data = [{"message": "Query executed successfully"}]
            
            return {
                "data": data,
                "query_type": "READ" if is_read_only else "WRITE" if needs_commit else "OTHER",
                "row_count": len(data)
            }
            
        except Exception as e:
            try:
                db.rollback()
            except:
                pass
            
            error_msg = str(e)
            if "Invalid column name" in error_msg:
                raise HTTPException(status_code=400, detail=f"Column not found: {error_msg}")
            elif "Invalid object name" in error_msg:
                raise HTTPException(status_code=400, detail=f"Table/object not found: {error_msg}")
            elif "Syntax error" in error_msg:
                raise HTTPException(status_code=400, detail=f"SQL syntax error: {error_msg}")
            else:
                raise HTTPException(status_code=400, detail=f"Query execution failed: {error_msg}")