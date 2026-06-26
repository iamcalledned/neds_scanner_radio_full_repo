"""Run the GPU Model Manager web server."""
import uvicorn

from ..core.config import DEFAULT_WEB_HOST, DEFAULT_WEB_PORT
from ..web.app import create_app

if __name__ == "__main__":
    uvicorn.run(create_app(), host=DEFAULT_WEB_HOST, port=DEFAULT_WEB_PORT, log_level="info")
