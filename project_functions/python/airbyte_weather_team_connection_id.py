
import requests
import json
import os

WORKSPACE_ID = os.getenv("WORKSPACE_ID")
AIRBYTE_API = os.getenv("AIRBYTE_API")

# Récupérer les connections
resp = requests.post(
    f"{AIRBYTE_API}/connections/list",
    json={"workspaceId": WORKSPACE_ID}
)
resp.raise_for_status()
data = resp.json()

# Filtrer par tag
filtered_connections = [
    {
        "connectionId": conn["connectionId"],
        "name": conn["name"],
        "tags": [tag["name"] for tag in conn.get("tags", [])]
    }
    for conn in data.get("connections", [])
    if any(tag.get("name") == "weather-team" for tag in conn.get("tags", []))
]

# store filtered connections
with open("./airbyte/weather_connections.json", "w") as f:
    json.dump(filtered_connections, f, indent=2, ensure_ascii=False)
