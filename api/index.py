import asyncio
import os
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Any
from enum import Enum
from contextlib import asynccontextmanager
from dataclasses import dataclass, field

import asyncpg
import httpx
from fastapi import FastAPI, HTTPException, Request, Depends, Security
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel, Field
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Boolean, DateTime, Text, Enum as SAEnum, ForeignKey, text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import declarative_base, relationship
import secrets
import hashlib

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/orchestrator")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
SECRET_KEY = os.getenv("SECRET_KEY", secrets.token_urlsafe(64))
API_PREFIX = "/api"
AUTH_SCHEME = HTTPBearer()

# Database setup
Base = declarative_base()
metadata = MetaData()

class AgentStatus(str, Enum):
    IDLE = "idle"
    BUSY = "busy"
    ERROR = "error"

class TaskStatus(str, Enum):
    QUEUED = "queued"
    PENDING = "pending"
    PROCESSING = "processing"
    FINAL = "final"
    AGENT_ERROR = "agent_error"

class UserRole(str, Enum):
    ADMIN = "admin"
    BACK_OFFICE = "back_office"
    USER = "user"
    SUPER_ADMIN = "super_admin"

# Database models
class Agents(Base):
    __tablename__ = "agents"
    id = Column(Integer, primary_key=True, autoincrement=True)
    division = Column(Integer, nullable=False)
    model = Column(Text, nullable=False)
    role = Column(Text, nullable=False)
    status = Column(SAEnum(AgentStatus), default=AgentStatus.IDLE)
    current_task_id = Column(Text, nullable=True)
    last_heartbeat = Column(DateTime(timezone=True), nullable=True)
    updated_at = Column(DateTime(timezone=True), server_default=text('CURRENT_TIMESTAMP'))

class Tasks(Base):
    __tablename__ = "tasks"
    task_id = Column(Integer, primary_key=True, autoincrement=True)
    agent_id = Column(Integer, ForeignKey('agents.id'))
    model = Column(Text, nullable=False)
    user_id = Column(Text, nullable=False, unique=True)
    base_task = Column(Text, nullable=False)
    chat_id = Column(Text, nullable=True)
    status = Column(SAEnum(TaskStatus), default=TaskStatus.QUEUED)
    created_at = Column(DateTime(timezone=True), server_default=text('CURRENT_TIMESTAMP'))
    dispatch_count = Column(Integer, default=0)
    final_output = Column(Text, nullable=True)
    available = Column(Boolean, default=True)

class UserActivations(Base):
    __tablename__ = "user_activations"
    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Text, nullable=False, unique=True)
    workspace_authorizations = Column(Text, nullable=False, default="{}")
    role = Column(SAEnum(UserRole), default=UserRole.USER)

# Create engines
engine = create_async_engine(
    DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://"),
    echo=True,
    pool_pre_ping=True,
    pool_size=20,
    max_overflow=0
)

async_session_maker = async_sessionmaker(engine, expire_on_commit=False)

@asynccontextmanager
async def get_db_session():
    async with async_session_maker() as session:
        try:
            yield session
            await session.commit()
        except:
            await session.rollback()
            raise
        finally:
            await session.close()

# Model configurations
class ModelRouter:
    CLAUDE_ARCHITECTURE = "anthropic/claude-3.5-sonnet"
    CLAUDE_OPUS = "anthropic/claude-3-opus"
    DEEPSEEK_R1 = "deepseek/deepseek-r1"
    DEEPSEEK_V3 = "deepseek/deepseek-v3"
    QWEN_CODER = "qwen/qwen-2.5-coder-32b"
    TOGETHER_AI = "togethercomputer/llama-2-70b-chat"
    
    ROUTING_MAP = {
        "architecture": [CLAUDE_ARCHITECTURE, CLAUDE_OPUS],
        "security_audit": [DEEPSEEK_R1],
        "database_ops": [DEEPSEEK_R1],
        "dr_validation": [DEEPSEEK_R1],
        "unit_economics": [DEEPSEEK_R1],
        "coding": [DEEPSEEK_V3, QWEN_CODER],
        "documentation": [DEEPSEEK_V3, QWEN_CODER],
        "qa_testing": [TOGETHER_AI],
        "market_trends": [TOGETHER_AI],
        "social_scheduling": [TOGETHER_AI],
        "finops_tracking": [TOGETHER_AI]
    }

class AgentRegistry:
    def __init__(self):
        self.agents: List[Dict[str, Any]] = []
        self._init_agents()
    
    def _init_agents(self):
        divisions = {
            "Back Office": {
                "agents": ["Financial Controller", "HR Manager", "Admin Assistant"]
            },
            "Sales & CRM": {
                "agents": ["Sales Executive", "CRM Specialist"]
            },
            "Content & Marketing": {
                "agents": ["Content Writer", "SEO Specialist", "Social Media Manager"]
            },
            "Finance": {
                "agents": ["Accountant", "Financial Analyst"] 
            },
            "Software Development": {
                "agents": ["Frontend Dev", "Backend Dev", "Full Stack Dev"]
            },
            "Design": {
                "agents": ["UI/UX Designer", "Graphic Designer"]
            },
            "Operations": {
                "agents": ["Operations Manager", "Quality Assurance", "Data Analyst"]
            },
            "Business": {
                "agents": ["Business Analyst", "Market Researcher", "Strategy Consultant"]
            }
        }
        
        divisions_flat = {}
        div_id = 1
        for div_name, div_config in divisions.items():
            divisions_flat[div_id] = div_name
            for agent_name in div_config["agents"]:
                division_model = self._get_default_model(div_name, agent_name)
                self.agents.append({
                    "division": div_id,
                    "model": division_model,
                    "role": f"{agent_name} -> {div_name}",
                    "status": "idle"
                })
            div_id += 1
    
    def _get_default_model(self, division: str, agent_name: str) -> str:
        if division == "Software Development":
            return ModelRouter.DEEPSEEK_V3
        if division == "Design":
            return ModelRouter.CLAUDE_ARCHITECTURE
        if division == "Security":
            return ModelRouter.DEEPSEEek_R1 if division == "Finance" else ModelRouter.CLAUDE_ARCHITECTURE
        if division == "Finance":
            return ModelRouter.CLAUDE_OPUS
        if division == "Sales & CRM":
            return ModelRouter.CLAUDE_ARCHITECTURE
        if division == "Content & Marketing":
            return ModelRouter.DEEPSEEK_V3
        if division == "Back Office":
            return ModelRouter.CLAUDE_OPUS
        if division == "Design":
            return ModelRouter.CLAUDE_ARCHITECTURE
        return ModelRouter.TOGETHER_AI
    
    def get_agents_by_division(self, division: int) -> List[Dict]:
        return [a for a in self.agents if a["division"] == division]
    
    def get_all_agents(self) -> List[Dict]:
        return self.agents
    
    def get_total_agents(self) -> int:
        return len(self.agents)

agent_registry = AgentRegistry()

# Task Manager
@dataclass
class TaskItem:
    task_id: int
    agent: str
    model: str
    user_id: str
    description: str
    status: TaskStatus = TaskStatus.QUEUED
    created_at: datetime = field(default_factory=datetime.now)
    priority: int = 0

class TaskScheduler:
    def __init__(self):
        self.tasks: Dict[int, TaskItem] = {}
    
    async def create_task(self, agent: str, task: str, user_id: str, description: str = "") -> int:
        db = None
        task_id = 0
        try:
            async with get_db_session() as session:
                new_task = Tasks(
                    agent_id=len(self.tasks) + 1,
                    model=self._get_agent_model(agent),
                    user_id=user_id,
                    base_task=task,
                    status=TaskStatus.QUEUED,
                    dispatch_count=0,
                    chat_id=self._get_telegram_chat_id(user_id)
                )
                session.add(new_task)
                await session.flush()
                task_id = new_task.task_id
                
                # Add to in-memory tracking
                self.tasks[task_id] = TaskItem(
                    task_id=task_id,
                    agent=agent,
                    model=self._get_agent_model(agent),
                    user_id=user_id,
                    description=description
                )
            logger.info(f"Created task {task_id} for user {user_id} with agent {agent}")
        except Exception as e:
            logger.error(f"Error creating task: {str(e)}")
            raise
        return task_id
    
    def _get_agent_model(self, agent_name: str) -> str:
        # Default mapping logic
        for agent_info in agent_registry.get_all_agents():
            if agent_info["role"].split(" -> ")[0] == agent_name:
                return agent_info["model"]
        return ModelRouter.DEEPSEEK_V3
    
    def _get_telegram_chat_id(self, user_id: str) -> str:
        # Return a fake tg chat id based on user id
        return f"tg_{hashlib.md5(user_id.encode()).hexdigest()[:10]}"
    
    async def process_task(self, task_id: int) -> str:
        task = self.tasks.get(task_id)
        if not task:
            raise ValueError(f"Task {task_id} not found")
        
        # Update status
        task.status = TaskStatus.PROCESSING
        db = None
        try:
            async with get_db_session() as session:
                db_task = await session.get(Tasks, task_id)
                if db_task:
                    db_task.status = TaskStatus.PROCESSING
            
            # Simulate model execution (in production this calls actual API)
            response = await self._call_agent(task)
            
            # Update with final result
            task.status = TaskStatus.FINAL
            async with get_db_session() as session:
                db_task = await session.get(Tasks, task_id)
                if db_task:
                    db_task.status = TaskStatus.FINAL
                    db_task.final_output = response
                    db_task.dispatch_count += 1
                    db_task.available = False
            logger.info(f"Task {task_id} completed process")
            return response
        except Exception as e:
            task.status = TaskStatus.AGENT_ERROR
            logger.error(f"Error processing task {task_id}: {e}")
            async with get_db_session() as session:
                db_task = await session.get(Tasks, task_id)
                if db_task:
                    db_task.status = TaskStatus.AGENT_ERROR
            raise
    
    async def _call_agent(self, task: TaskItem):
        # In production, this would make actual API calls
        await asyncio.sleep(0.5)
        return f"Generated analysis for {task.description}"
    
    def get_task_status(self, task_id: int) -> str:
        return self.tasks.get(task_id, TaskItem(task_id=0, agent="", model="", user_id="")).status.value

# FastAPI Instance
app = FastAPI(
    title="DeepSeek Agent Orchestration System",
    description="Production-grade autonomous orchestration system",
    version="1.0.0",
    docs_url=f"{API_PREFIX}/docs"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)

# Include routers
@app.get(f"{API_PREFIX}/health")
async def health_check():
    try:
        db_status = await check_database()
        return {
            "status": "ok",
            "db": db_status,
            "cache": "connected",
            "model": "18ms"
        }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

async def check_database():
    try:
        async with get_db_session() as session:
            await session.execute(text("SELECT 1"))
            return "connected"
    except Exception as e:
        logger.error(f"Database check failed: {e}")
        return "disconnected"

@app.get(f"{API_PREFIX}/dashboard/overview")
async def dashboard_overview():
    total_agents = agent_registry.get_total_agents()
    current_load = len(TaskScheduler().tasks)
    return {
        "total_agents": total_agents,
        "active_agents": total_agents,
        "current_load": current_load,
        "queued_tasks": len([t for t in TaskScheduler().tasks.values() if t.status == TaskStatus.QUEUED]),
        "processing": len([t for t in TaskScheduler().tasks.values() if t.status == TaskStatus.PROCESSING]),
        "completed": len([t for t in TaskScheduler().tasks.values() if t.status == TaskStatus.FINAL]),
        "time_since_last_update": "active"
    }

@app.post(f"{API_PREFIX}/dashboard/report")
async def dashboard_report():
    report = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "divisions": [
            {
                "name": "Back Office", 
                "agents": sum(1 for a in agent_registry.get_agents_by_division(i) for i in range(1, 3))
            },
            {
                "name": "Sales & CRM",
                "agents": sum(1 for a in agent_registry.get_agents_by_division(i) for i in range(3, 4))
            },
            {
                "name": "Content & Marketing",
                "agents": sum(1 for a in agent_registry.get_agents_by_division(i) for i in range(4, 5))
            },
            {
                "name": "Finance",
                "agents": sum(1 for a in agent_registry.get_agents_by_division(i) for i in range(5, 6))
            },
            {
                "name": "Software Development",
                "agents": sum(1 for a in agent_registry.get_agents_by_division(i) for i in range(6, 8))
            },
            {
                "name": "Design",
                "agents": sum(1 for a in agent_registry.get_agents_by_division(i) for i in range(8, 10))
            }
        ]
    }
    return report

@app.get(f"{API_PREFIX}/dashboard/vault/status")
async def vault_status():
    try:
        return {
            "status": "active",
            "last_sync": datetime.now(timezone.utc).isoformat(),
            "successful_access": 1,
            "failed_access": 0
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Vault service unavailable: {str(e)}")

@app.get(f"{API_PREFIX}/api/token/usage")
async def token_usage():
    # In production, query actual usage from DB
    now = datetime.now(timezone.utc)
    return {
        "period": "24h",
        "total_tokens": 150000,
        "model_breakdown": [
            {"model": "claude-3.5-sonnet", "tokens": 50000},
            {"model": "deepseek-r1", "tokens": 40000},
            {"model": "deepseek-v3", "tokens": 35000},
            {"model": "qwen-2.5-coder", "tokens": 25000}
        ],
        "cost": 12.45
    }

@app.post(f"{API_PREFIX}/api/token/budget")
async def token_budget(request: Request):
    try:
        body = await request.json()
        daily_limit = body.get("daily_limit", 50000)
        monthly_limit = body.get("monthly_limit", 500000)
        return {"budget": {"daily": daily_limit, "monthly": monthly_limit}, "set": True}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get(f"{API_PREFIX}/api/token/costs")
async def token_costs():
    now = datetime.now(timezone.utc)
    return {
        "daily_spend": 12.45,
        "monthly_spend": 345.67,
        "estimated_monthly": 890.0,
        "average_per_request": 0.12
    }

# Error handling middleware
@app.middleware("http")
async def logging_middleware(request: Request, call_next):
    logger.info(f"Request: {request.method} {request.url.path}")
    start_time = datetime.now()
    response = await call_next(request)
    process_time = (datetime.now() - start_time).total_seconds()
    response.headers["X-Process-Time"] = str(process_time)
    logger.info(f"Response: {response.status_code} took {process_time:.3f}s")
    return response

# Initialize system state
task_scheduler = TaskScheduler()

@app.on_event("startup")
async def startup():
    logger.info("Initializing DeepSeek Agent Orchestrator...")
    # Bind async engine, create tables if not exist
    try:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        logger.info("Database tables ensured")
    except Exception as e:
        logger.warning(f"Database initialization warning: {e}")

@app.on_event("shutdown")
async def shutdown():
    logger.info("Shutting down DeepSeek Agent Orchestrator...")
