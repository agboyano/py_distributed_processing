__version__ = "0.0.1a"

import logging

# Library best practice: attach a NullHandler and let the application
# configure logging (handlers, level and format).
logging.getLogger(__name__).addHandler(logging.NullHandler())
