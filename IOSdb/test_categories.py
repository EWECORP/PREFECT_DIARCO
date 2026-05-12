import requests
from IOSdb.config.settings import load_settings
from IOSdb.flows.retry_utils import get_token

token = get_token()
headers = {"Authorization": f"Bearer {token}"}
settings = load_settings()

# Un solo item de prueba
payload = [
    {
        "id": "13",
        "name": "VARIOS",
        "path": None,
        "parent_id": "1"
    }
]

response = requests.post(
    settings.api.categories_url,
    json=payload,
    headers=headers,
    timeout=settings.api.request_timeout_seconds,
)
print(f"Status: {response.status_code}")
print(f"Respuesta: {response.text}")
