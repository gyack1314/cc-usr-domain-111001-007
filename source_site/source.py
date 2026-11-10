"""合成来源行为，可直接调用或通过本地 HTTP 访问。"""
import hashlib
import json
import threading
from pathlib import Path
from urllib.parse import parse_qs

DATA = Path(__file__).resolve().parents[1] / "sources"


def json_file(name):
    return json.loads((DATA / name).read_text(encoding="utf-8"))


class Source:
    def __init__(self):
        self.lock = threading.RLock()
        self.calls = []
        self.phase = "base"
        self.state = {}

    def request(self, method, path, query=None, headers=None, body=b""):
        query, headers = query or {}, headers or {}
        self.calls.append({"method": method, "path": path, "query": query})
        if path == "/__control" and method == "POST":
            control = json.loads(body or b"{}")
            self.phase = control.get("phase", self.phase)
            self.state.update(control.get("state", {}))
            return 200, {}, {"phase": self.phase}
        if path == "/__calls":
            return 200, {}, self.calls[:-1]
        return self.dispatch(method, path, query, headers, body)

    def dispatch(self, method, path, q, h, body):
        if path == "/tasks":
            return 200, {}, json_file("tasks.json")
        script = json_file("responses.json")
        items = script.get(path)
        if items is None:
            return 404, {}, {"error": "来源路径不存在"}
        pos = self.state.get(path, 0)
        self.state[path] = pos+1
        item = items[min(pos, len(items)-1)]
        return item["status"], item.get("headers", {}), item["body"]
