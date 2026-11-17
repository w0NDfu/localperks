import hashlib
import re
from dataclasses import asdict, dataclass, field
from pathlib import Path


@dataclass
class Chunk:
    id: str
    doc_id: str
    text: str
    source: str
    title: str
    metadata: dict = field(default_factory=dict)

    def to_dict(self):
        return asdict(self)


def chunks(text, source, *, title="", metadata=None, size=600, overlap=80):
    if size < 100 or overlap < 0 or overlap >= size:
        raise ValueError("Invalid chunking limits")
    text = text.replace('\r\n', '\n').strip()
    doc_id = hashlib.sha256(source.encode()).hexdigest()[:24]
    output, start = [], 0
    while start < len(text):
        end = min(start + size, len(text))
        if end < len(text):
            boundary = text.rfind('\n', start + size // 2, end)
            if boundary > start:
                end = boundary
        body = text[start:end].strip()
        if body:
            digest = hashlib.sha256((source + '\0' + str(start) + '\0' + body).encode()).hexdigest()[:32]
            output.append(Chunk(digest, doc_id, body, source, title or source, metadata or {}))
        if end == len(text):
            break
        start = end - overlap
    return output


def parse_file(path: Path, source: str):
    if path.suffix.lower() not in {'.md', '.txt'}:
        raise ValueError("仅接受UTF-8 Markdown/TXT文档")
    text = path.read_text(encoding='utf-8-sig')
    headings = re.findall(r'^#{1,3}\s+(.+)$', text, re.M)
    return chunks(text, source, title=headings[0] if headings else path.stem, metadata={"kind": "documentation"})
