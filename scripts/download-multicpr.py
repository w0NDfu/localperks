"""Download upstream public files without changing labels or touching the live KB."""
import hashlib
import json
from pathlib import Path
import httpx

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "artifacts/customer-assistant/datasets/multicpr-ecom"
BASE = "https://raw.githubusercontent.com/Alibaba-NLP/Multi-CPR/main/data/ecom/"


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    manifest = {"upstream": "https://github.com/Alibaba-NLP/Multi-CPR", "files": []}
    with httpx.Client(timeout=90, follow_redirects=True) as client:
        for name in ("dev.query.txt", "qrels.dev.tsv", "corpus.tsv"):
            path = OUT / name
            if not path.exists():
                with client.stream("GET", BASE + name) as response:
                    response.raise_for_status()
                    with path.with_suffix(path.suffix + ".part").open("wb") as target:
                        for chunk in response.iter_bytes():
                            target.write(chunk)
                path.with_suffix(path.suffix + ".part").replace(path)
            digest = hashlib.sha256(path.read_bytes()).hexdigest()
            manifest["files"].append({"name": name, "url": BASE + name, "sha256": digest, "bytes": path.stat().st_size})
            print(name, path.stat().st_size, digest, flush=True)
    (OUT / "download-manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
