import uvicorn
from app.main import app
from app.core.config import config


def main():
    uvicorn.run(
        "app.main:app",
        host=config.host,
        port=config.port,
        reload=config.reload,
        log_level=config.log_level.lower()
    )


if __name__ == "__main__":
    main()
