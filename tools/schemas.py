# tools/schemas.py
"""Event contracts: export, drift check, and Schema Registry compatibility.

    python tools/schemas.py export   --service services/queue_service   # write contracts/<topic>/*.json
    python tools/schemas.py check    --service services/queue_service   # fail if contracts/ is out of date
    python tools/schemas.py compat   --registry http://localhost:8081   # fail if any schema breaks compatibility
    python tools/schemas.py register --registry http://localhost:8081   # compat check, then register new versions

export/check import the service's app/events/catalog.py (run them with that service's requirements installed).
compat/register only read the JSON files and use the standard library, so CI can run them anywhere.

Subject naming = TopicRecordNameStrategy: "<topic>-<EventType>", e.g. "queue.events-TicketReserved",
because one topic carries several event types, each with its own schema.
"""
import argparse
import importlib
import json
import sys
import urllib.error
import urllib.request
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
CONTRACTS = REPO / "contracts"
DRAFT = "https://json-schema.org/draft/2020-12/schema"
COMPATIBILITY = "BACKWARD"


# ---------------------------------------------------------------- export / check

def render(service_dir: Path) -> tuple[str, dict[str, str]]:
    """Generate {relative path: file content} for every event in the service's catalog."""
    sys.path.insert(0, str(service_dir.resolve()))
    catalog = importlib.import_module("app.events.catalog")
    files: dict[str, str] = {}
    for event_type, model in catalog.EVENT_MODELS.items():
        schema = {"$schema": DRAFT, **model.model_json_schema()}
        files[f"{catalog.TOPIC}/{event_type.value}.schema.json"] = json.dumps(schema, indent=2, sort_keys=True) + "\n"
    for name, event in catalog.examples().items():
        files[f"{catalog.TOPIC}/{name}.example.json"] = json.dumps(
            event.model_dump(mode="json"), indent=2, sort_keys=True) + "\n"
    return catalog.TOPIC, files


def export(service_dir: Path) -> int:
    topic, files = render(service_dir)
    folder = CONTRACTS / topic
    folder.mkdir(parents=True, exist_ok=True)
    for stale in set(p.name for p in folder.glob("*.json")) - {Path(f).name for f in files}:
        (folder / stale).unlink()
        print(f"removed  contracts/{topic}/{stale}")
    for rel, text in files.items():
        (CONTRACTS / rel).write_text(text)
        print(f"wrote    contracts/{rel}")
    return 0


def check(service_dir: Path) -> int:
    topic, files = render(service_dir)
    problems = []
    for rel, text in files.items():
        path = CONTRACTS / rel
        if not path.exists():
            problems.append(f"missing  contracts/{rel}")
        elif path.read_text() != text:
            problems.append(f"changed  contracts/{rel}")
    for path in (CONTRACTS / topic).glob("*.json"):
        if f"{topic}/{path.name}" not in files:
            problems.append(f"stale    contracts/{topic}/{path.name}")
    for p in problems:
        print(p)
    if problems:
        print(f"\nThe catalog and contracts/ disagree. Run: python tools/schemas.py export --service {service_dir}")
        return 1
    print(f"contracts/{topic} is up to date ({len(files)} files)")
    return 0


# ---------------------------------------------------------------- Schema Registry (Confluent REST API)

def schema_files() -> list[tuple[str, Path]]:
    """(subject, path) for every committed schema."""
    return [(f"{p.parent.name}-{p.name.removesuffix('.schema.json')}", p)
            for p in sorted(CONTRACTS.glob("*/*.schema.json"))]


def call(registry: str, method: str, path: str, body: dict | None = None) -> tuple[int, dict]:
    request = urllib.request.Request(
        registry.rstrip("/") + path, method=method,
        data=json.dumps(body).encode() if body is not None else None,
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            return response.status, json.loads(response.read() or b"{}")
    except urllib.error.HTTPError as err:
        return err.code, json.loads(err.read() or b"{}")


def is_compatible(registry: str, subject: str, schema_text: str) -> tuple[bool, list[str]]:
    status, body = call(registry, "POST", f"/compatibility/subjects/{subject}/versions/latest?verbose=true",
                        {"schemaType": "JSON", "schema": schema_text})
    if status == 404:                      # subject does not exist yet: a first version is always fine
        return True, ["new subject"]
    if status != 200:
        raise RuntimeError(f"{subject}: registry answered {status}: {body}")
    return body.get("is_compatible", False), body.get("messages", [])


def compat(registry: str) -> int:
    failed = 0
    for subject, path in schema_files():
        ok, messages = is_compatible(registry, subject, path.read_text())
        print(f"{'OK  ' if ok else 'FAIL'}  {subject}")
        if not ok:
            failed += 1
            for m in messages:
                print(f"        {m}")
    if failed:
        print(f"\n{failed} schema(s) are not {COMPATIBILITY}-compatible with the registered version.")
    return 1 if failed else 0


def register(registry: str) -> int:
    if compat(registry):
        print("Nothing registered.")
        return 1
    for subject, path in schema_files():
        call(registry, "PUT", f"/config/{subject}", {"compatibility": COMPATIBILITY})
        status, body = call(registry, "POST", f"/subjects/{subject}/versions",
                            {"schemaType": "JSON", "schema": path.read_text()})
        if status != 200:
            print(f"FAIL  {subject}: {status} {body}")
            return 1
        _, versions = call(registry, "GET", f"/subjects/{subject}/versions")
        print(f"registered  {subject}  id={body['id']}  versions={versions}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("command", choices=["export", "check", "compat", "register"])
    parser.add_argument("--service", type=Path, help="service folder, for export/check")
    parser.add_argument("--registry", default="http://localhost:8081", help="Schema Registry URL, for compat/register")
    args = parser.parse_args()
    if args.command in ("export", "check"):
        if not args.service:
            parser.error("--service is required for export/check")
        return (export if args.command == "export" else check)(args.service)
    return (compat if args.command == "compat" else register)(args.registry)


if __name__ == "__main__":
    sys.exit(main())