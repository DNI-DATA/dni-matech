import hashlib
import os
import re
import socket
import sys
import pickle

from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import Flow
from googleapiclient.discovery import build


PORT = 8080
REDIRECT_URI = f"http://127.0.0.1:{PORT}"
CLIENT_SECRETS_FILE = "admob_client_secrets.json"

API_NAME = "admob"
API_VERSION = "v1"
API_SCOPE = "https://www.googleapis.com/auth/admob.readonly"

TOKEN_FILE = 'token.pickle'

class GoogleAdMob:
    def __init__(
        self, client_secrets_file=CLIENT_SECRETS_FILE, 
    ):
        self.client_secrets = os.path.join(os.path.dirname(__file__), CLIENT_SECRETS_FILE)
        self.service = None
        
    
    def authenticate(
        api_name=API_NAME,
        api_version=API_VERSION,
        api_scopes=[API_SCOPE],
    ):