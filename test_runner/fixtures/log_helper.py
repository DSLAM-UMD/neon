import logging
import logging.config
import time

# timestamp in UTC+-00:00 aka GMT
class UTCFormatter(logging.Formatter):
    converter = time.gmtime

LOGGING = {
    "version": 1,
    "formatters": {
    "defaultFormatter": {
        "()": UTCFormatter,
        "format": "%(asctime)s.%(msecs)-3d %(levelname)s [%(filename)s:%(lineno)d] %(message)s",
        "datefmt": "%Y-%m-%d %H:%M:%S"
    }
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "defaultFormatter",
            "level": "DEBUG",
            "stream": "ext://sys.stderr"
        }
    },
    "loggers": {
        "root": {
            "level": "DEBUG",
            "handlers": ["console"]
        },
        "root.wal_acceptor_async": { 
            "level": "INFO" # lot of logs on DEBUG level
        }
    }
}

logging.config.dictConfig(LOGGING)
