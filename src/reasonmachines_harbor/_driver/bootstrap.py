"""Public identity discovery."""
from uuid import UUID


def resource_id(value):
    if not isinstance(value, str):
        raise ValueError("API returned a missing or invalid resource ID")
    try:
        return str(UUID(value))
    except ValueError:
        raise ValueError("API returned a missing or invalid resource ID") from None


async def resolve_organization(request, expected_org=""):
    body = (await request("GET", "/v3/self")).json()
    org = resource_id(body.get("org_id") if isinstance(body, dict) else None)
    if expected_org and resource_id(expected_org) != org:
        raise ValueError("Configured organization does not match the API key organization")
    return org
