"""资料采集插件的同步发送接口。"""
from urllib.error import HTTPError
from urllib.request import urlopen


def fetch(task, base_url):
    try:
        response = urlopen(base_url + task["path"], timeout=5)
    except HTTPError as exc:
        response = exc
    with response:
        return response.status, dict(response.headers), response.read()
