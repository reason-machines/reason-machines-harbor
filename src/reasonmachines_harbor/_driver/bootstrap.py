"""Public identity discovery and race-safe benchmark project selection."""
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


def _is_benchmark_project(body, slug, name):
    return (isinstance(body, dict) and body.get("slug") == slug
            and body.get("name") == name and body.get("visibility") == "workspace"
            and body.get("resources") == [])


def benchmark_project(body, slug, name):
    if not _is_benchmark_project(body, slug, name):
        raise ValueError(f"{name} must be a workspace-visible, resource-free project")
    return resource_id(body.get("id"))


async def find_project(request, path, slug, name):
    body = (await request("GET", path)).json()
    if (not isinstance(body, dict) or not isinstance(body.get("items"), list)
            or body.get("has_next_page") is not False):
        raise ValueError("Invalid or incomplete project listing")
    matches = [item for item in body["items"] if isinstance(item, dict) and item.get("slug") == slug]
    if len(matches) > 1:
        raise ValueError("Ambiguous benchmark project")
    # Any other project holding the slug (private, renamed, resource-bound) is
    # a collision like a hidden one: it reserves the slug, is never reused or
    # changed, and the caller moves on to the next stable slug.
    if not matches or not _is_benchmark_project(matches[0], slug, name):
        return None
    return benchmark_project(matches[0], slug, name)


async def resolve_benchmark_project(request, org, name):
    """Find or create the shared project `name`, e.g. "Pier Benchmarks" at slug pier-benchmarks."""
    from .driver import ReasonAPIError

    base_slug = name.lower().replace(" ", "-")
    path = f"/v3/organizations/{resource_id(org)}/projects"
    for number in range(1, 11):
        slug = base_slug if number == 1 else f"{base_slug}-{number}"
        existing = await find_project(request, path, slug, name)
        if existing:
            return existing
        try:
            response = await request("POST", path, {
                "name": name, "slug": slug, "visibility": "workspace",
                "kind": "remote", "cloud_enabled": False, "resources": [], "source_folders": [],
            })
        except ReasonAPIError as error:
            if error.status != 409 or error.code != "project_slug_conflict":
                raise
            # A concurrent trial may have won this slug. Archived/private
            # projects can also reserve it: leave them untouched and use the
            # next stable slug so concurrent trials still converge.
            existing = await find_project(request, path, slug, name)
            if existing:
                return existing
            if number == 10:
                raise RuntimeError(
                    f"All 10 benchmark project slugs ('{base_slug}' through '{base_slug}-10') "
                    "are already in use in this workspace, but no active workspace-visible "
                    "benchmark project is accessible to this API key. The projects may be private "
                    "or archived. Ask a workspace admin to grant the API key's user access and "
                    "make a benchmark project workspace-visible, or restore it if archived, then retry."
                ) from error
            continue
        return benchmark_project(response.json(), slug, name)
