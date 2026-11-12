# 多来源消防资料采集的共享配额调度资料源

本项目保存本地合成资料及对应的来源站行为，供资料接入开发使用。内容取自虚构场景，不代表真实活动、人员、设施或设备参数。来源服务只模拟公开读取与指定的表单行为，不包含资料采集端。

## 环境与运行

使用 Python 3.11 及以上版本，仅依赖标准库。来源只监听本机地址，端口可以通过参数调整。两个终端分别启动不同端口可以表示两个独立来源。

```sh
python -m source_site --port 8090
```

`source_site/source.py` 管理来源响应，`source_site/__main__.py` 是 HTTP 适配层，`sources/` 保存原文与接口约定。状态只存在来源进程内，重启来源会恢复初始样本。`/__control` 和 `/__calls` 属于本地运维入口，资料接入端的正常操作不使用它们。运维端可以通过 POST `/__control` 提供 JSON 对象改变 phase 或更新 state。phase 列表与业务接口见下文，细节见 `sources/contract.json`。

- `GET /tasks`：返回合成任务
- `GET /north,/south`：按responses.json逐次响应，末项保持
- `sample_plugin`：同步fetch(task,base_url)返回status,headers,body；调度器可包装但保持参数
- `time`：逻辑秒，可注入时钟；HTTP-date以UTC解析

用 Python 的 HTTP 客户端推进来源状态，例如：

```sh
python -c 'import urllib.request; r=urllib.request.Request("http://127.0.0.1:8090/__control", data=b"{\"phase\":\"base\"}", headers={"Content-Type":"application/json"}); print(urllib.request.urlopen(r).read().decode())'
```

## 请求调度层（scheduler 包）

`scheduler/` 在不改动采集插件解析器的前提下，提供统一请求调度。插件仍以
`submit(task)` 提交任务，`sample_plugin.fetch(task, base_url)` 的签名与
返回三元组保持不变，由 `PluginSender` 适配为发送器接口；需要在途语义时
再用 `ThreadedSender` 包装。

- 令牌桶按 `quota_group` 配置容量与每秒补充速率，同组跨 source 共享；
  仅在请求真正进入发送器前扣费，排队取消与过期不收费，已发送失败不退款；
  `cost` 超过组容量入队即返回 `REJECTED`。
- source 配置并发上限；同优先级来源轮转、来源内按入队顺序；低优先级每
  等待 10 逻辑秒老化一级，封顶 3，原始优先级 0..3。
- `not_before`/`deadline` 由可注入时钟（`ManualClock`/`SystemClock`）驱动；
  超过 deadline 且未发送的任务结束为 `EXPIRED`，不占用连接。
- 429/503 的 `Retry-After`（delta-seconds 或 HTTP-date，UTC 解析）产生
  组级冷却，在途请求允许结束，冷却期同组另一域名不能偷跑；无合法
  Retry-After 时按 `backoff.base/cap` 指数退避，`max_retries` 耗尽转失败。
  连接失败与 HTTP 响应共用同一重试预算，但不触发组冷却。
- 结果按 `task_id` 幂等接收；重启后在途任务转为 `PENDING_VERIFICATION`，
  由插件通过 `verify_decision(task_id, "retry"|"abandon")` 决定，不会自动重发。
- 持久化为原子 JSON（临时文件 + `os.replace`），落盘全部使用 UTC 绝对秒；
  单调时钟仅用于进程内。重启以新锚点按 `max(0, wall_now - wall_saved)`
  计算流逝，墙钟回退时流逝为 0，不凭空补 token、不提前结束冷却。

演示与联调（均不需要真实等待或外网凭据）：

```sh
python -I demo.py               # 虚拟时钟：共享额度/插队/老化/过期/组冷却/重启
python -I http_integration.py   # 起两个本地来源实例做真实 HTTP 联调
```

## 开发检查

以下检查覆盖来源协议、异常响应、真实本地 HTTP 启动以及调度层全部不变量。
检查不会要求访问公网，也不会写入业务状态目录（持久化测试使用临时目录）。

```sh
python -I -m unittest discover -s tests -t . -v
python -m compileall -q .
```

容器环境也可以复现相同检查：

```sh
docker build -t quota-dispatch-source .
docker run --rm --network none quota-dispatch-source
```

来源的HTTP异常是合成协议行为，便于资料接入端开发和诊断。样本可扩展；业务数据、原始HTML以及接口响应不应被当作真实部署凭据或真实业务事实。
