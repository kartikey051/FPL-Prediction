"""
FPL Dashboard - Main FastAPI Application

This is the entry point for the application. It:
- Configures CORS
- Mounts static files
- Includes auth and dashboard routers
- Serves HTML templates for login/register/dashboard
"""

from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, RedirectResponse

from app.core.config import settings
from app.api.auth.router import router as auth_router
from app.api.dashboard.router import router as dashboard_router
from app.api.prediction.router import router as prediction_router
from app.db.models.user import ensure_users_table
from app.api.prediction.service import ensure_predictions_table
from Utils.logging_config import get_logger

logger = get_logger("main")

# Create FastAPI app
app = FastAPI(
    title=settings.APP_NAME,
    description="FPL Analytics Dashboard with JWT Authentication",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

# CORS configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Paths
BASE_DIR = Path(__file__).parent
TEMPLATES_DIR = BASE_DIR / "templates"
STATIC_DIR = BASE_DIR / "static"

# Mount static files
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

# Templates
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

# Include routers
app.include_router(auth_router)
app.include_router(dashboard_router)
app.include_router(prediction_router)


@app.on_event("startup")
async def startup_event():
    """Initialize database tables on startup."""
    logger.info("Starting FPL Dashboard application...")
    try:
        ensure_users_table()
        ensure_predictions_table()
        logger.info("Database tables initialized (users, predictions)")
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")
        # Don't raise - allow app to start even if DB is down
        # Dashboard will show errors when accessed


@app.get("/", response_class=HTMLResponse)
async def login_page(request: Request):
    """Serve the login page."""
    return templates.TemplateResponse("login.html", {"request": request})


@app.get("/register", response_class=HTMLResponse)
async def register_page(request: Request):
    """Serve the registration page."""
    return templates.TemplateResponse("register.html", {"request": request})


@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard_page(request: Request):
    """Serve the dashboard page."""
    return templates.TemplateResponse("dashboard.html", {"request": request})


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "app": settings.APP_NAME}


# Error handlers
@app.exception_handler(404)
async def not_found_handler(request: Request, exc):
    """Handle 404 errors."""
    return RedirectResponse(url="/")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)
