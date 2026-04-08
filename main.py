from __future__ import annotations

import os
import sys
from pathlib import Path

import uvicorn

BACKEND_DIR = Path(__file__).resolve().parent / 'backend'
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from app.server import app


if __name__ == '__main__':
    host = os.getenv('HOST', '127.0.0.1')
    port = int(os.getenv('PORT', '8100'))
    uvicorn.run('app.server:app', host=host, port=port, reload=os.getenv('APP_RELOAD', '0') in {'1', 'true', 'yes'})
