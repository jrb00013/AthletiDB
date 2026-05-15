#!/usr/bin/env python3
"""
AthletiDB Web Server Entry Point
Run this to start the web interface
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from web.main import app
import uvicorn

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    print(f"🏆 Starting AthletiDB Web Server on http://localhost:{port}")
    uvicorn.run(app, host="0.0.0.0", port=port)