"""Exercise the local asynchronous API; does not substitute for visual browser QA."""

import argparse
import csv
import io
import json
import time
import urllib.error
import urllib.request
from pathlib import Path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://127.0.0.1:8002")
    parser.add_argument("--output", type=Path, default=Path("tmp/http-verification.json"))
    args = parser.parse_args()

    def request(path, body=None):
        req = urllib.request.Request(
            args.base_url + path,
            data=json.dumps(body).encode() if body else None,
            headers={"Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(req, timeout=20) as response:
                return response.status, response.read(), response.headers
        except urllib.error.HTTPError as exc:
            return exc.code, exc.read(), exc.headers

    def wait(run_id):
        deadline = time.monotonic() + 180
        while time.monotonic() < deadline:
            status, payload, _ = request("/api/runs/" + run_id)
            assert status == 200
            result = json.loads(payload)
            if result["state"] != "running":
                return result
            time.sleep(0.5)
        raise AssertionError("The local job did not finish within the HTTP smoke budget.")

    status, payload, _ = request("/api/summary")
    assert status == 200
    for current in json.loads(payload)["runs"]:
        if current["state"] == "running":
            wait(current["run_id"])
    results = {}
    for action, strict in [
        ("diagnose", False),
        ("recover", False),
        ("recover", False),
        ("recover", True),
    ]:
        status, payload, _ = request("/api/jobs", {"action": action, "strict": strict})
        assert status == 202, (status, payload)
        run_id = json.loads(payload)["run_id"]
        conflict, _, _ = request("/api/jobs", {"action": "recover"})
        assert conflict == 409, conflict
        run = wait(run_id)
        label = "strict-blocked" if strict else action if action not in results else "replay"
        results[label] = {
            "run_id": run_id,
            "state": run["state"],
            "publication": run["report"].get("publication"),
        }
        if strict:
            assert run["state"] == "failed"
            assert run["report"]["error"]["code"] == "quality_gate"
        else:
            assert run["state"].startswith("succeeded")
        if action == "recover" and not strict:
            for kind in ["report", "logs", "csv"]:
                code, content, headers = request(f"/api/runs/{run_id}/download/{kind}")
                assert code == 200 and "attachment" in headers.get("Content-Disposition", "")
                if kind == "csv":
                    assert len(list(csv.DictReader(io.StringIO(content.decode())))) == 17
                else:
                    json.loads(content)
    status, payload, _ = request("/api/summary")
    summary = json.loads(payload)
    assert len(summary["lines"]) == 7
    assert summary["published"][0]["gross_minor"] == 51120
    assert results["replay"]["publication"]["action"] == "unchanged"
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(
            {
                "scope": "Actual local HTTP requests; not visual browser QA.",
                "checks": {
                    "asynchronous_dispatch": True,
                    "concurrent_request_409": True,
                    "saved_downloads_reopened": True,
                    "published_lines": 7,
                    "gross_minor": 51120,
                    "strict_gate_preserved_partition": True,
                },
                "runs": results,
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    print(
        "Verified HTTP dispatch, persisted polling, downloads, replay and blocking gate:",
        args.output,
    )


if __name__ == "__main__":
    main()
