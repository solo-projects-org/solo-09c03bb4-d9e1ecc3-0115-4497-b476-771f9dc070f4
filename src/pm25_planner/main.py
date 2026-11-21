"""本地开发入口：python -m pm25_planner.main 或 uvicorn pm25_planner.main:app。"""
from __future__ import annotations

import uvicorn

from .api import app

__all__ = ["app"]

if __name__ == "__main__":
    uvicorn.run("pm25_planner.main:app", host="0.0.0.0", port=8000, reload=False)
