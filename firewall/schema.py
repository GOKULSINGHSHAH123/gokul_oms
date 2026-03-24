"""
schema.py
---------
MongoDB database and collection name registry for the firewall package.
"""


class Databases:
    ALGO_SIGNALS    = "algo_signals"
    FINAL_RESPONSE  = "Info"
    RMS             = "Info"
    ERRORS          = "error_db"


class Collections:
    ALGO_CONFIG     = "algo_rms_params"
    SEGMENT_CONFIG  = "segment_rms_params"
    CLIENT_RMS      = "client_rms_params"
