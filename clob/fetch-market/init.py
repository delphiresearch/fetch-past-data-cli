import os
from dotenv import load_dotenv
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import ApiCreds
from py_clob_client.constants import AMOY
load_dotenv()

def main():
    host = "https://clob.polymarket.com"
    key = os.getenv("PK")
    chain_id = AMOY
    client = ClobClient(host, key=key, chain_id=chain_id)

    print(client.get_clob_auth_domain())

    print(client.create_api_key())

main()