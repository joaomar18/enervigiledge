import logging

logging.basicConfig(level=logging.INFO)
logging.getLogger("pymodbus").setLevel(logging.CRITICAL)
logger = logging.getLogger("enervigil_edge")
