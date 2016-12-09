BASE_CONFIG = {
    'version': 1,
    'loggers': {
        'pit': {
            'handlers': ['console'],
            'level': 'DEBUG',
        }
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'stream': 'ext://sys.stdout',
            'formatter': 'verbose',
        }
    },
    'formatters': {
        'verbose': {
            'format': '%(levelname)s [%(asctime)s] %(message)s in '
                      '%(filename)s:%(lineno)d'
        }
    }
}
