# 依赖公告登记（当前）

本页记录本轮按锁文件核验过的依赖公告状态，以及**已知但当前无法靠升级解决**的残留项。
它描述的是 2026-11-16 的快照，不是持续同步的漏洞库；复核方式是重跑下面写的查询命令。

核验方法：把 `Cargo.lock` 里全部带 `source = "registry..."` 的包（本轮 320 个）连同解析版本
一次性提交给 OSV，再对前端 `web/package-lock.json` 的实际解析版本单独查询。

```sh
# 生成查询体
python3 - <<'EOF' > /tmp/osv-batch.json
import re, json
s = open('Cargo.lock').read()
pkgs = re.findall(r'\[\[package\]\]\nname = "([^"]+)"\nversion = "([^"]+)"\nsource = "registry', s)
print(json.dumps({"queries": [{"package": {"name": n, "ecosystem": "crates.io"}, "version": v} for n, v in pkgs]}))
EOF
curl -s -X POST -d @/tmp/osv-batch.json https://api.osv.dev/v1/querybatch
```

## 当前状态

| 组件 | 解析版本 | 公告 | 是否进生产 | 处置 |
| --- | --- | --- | --- | --- |
| `hyper` | 1.12.0 | 无 | 是 | — |
| `reqwest` | 0.12.28 | 无 | 是 | — |
| `rustls` | 0.23.46 | 无 | 是 | — |
| `sqlx` | 0.8.6 | 无 | 是 | — |
| `serde` | 1.0.229 | 无 | 是 | — |
| `tokio` | 1.54.1 | 无 | 是 | — |
| `image` | 0.25.10 | 无 | 是 | — |
| `rustix` | 1.1.6 | RUSTSEC-2026-0486（已修） | 否 | 由 `tempfile` 引入；`tempfile` 已移入 `[dev-dependencies]`，正常依赖图中不再出现 |
| `rsa` | 0.9.10 | RUSTSEC-2023-0071 | 否 | 仅被未编译的 `sqlx-mysql` 拉入；`sqlx` 用 `default-features = false`，只启 `sqlite` |
| `libsqlite3-sys` | 0.30.1（内置 SQLite 3.46.0） | RUSTSEC-2026-0459 / CVE-2026-119375 | **是** | **无法升级，见下节残留项** |
| `vite` | 5.4.21 | 8 条（dev server 类） | 否 | 前端只在开发期跑；镜像不装 node，`web/dist` 不进仓库 |
| `vitest` / `esbuild` | 2.1.9 / 0.21.5 | 2 条 / 1 条（测试期类） | 否 | 同上 |
| `react` / `react-dom` / `jsdom` / `typescript` | — | 无 | 仅静态产物 | — |

## 残留项：内置 SQLite 低于修复版本

`sqlx` 走 `bundled` 特性（`sqlx-sqlite` → `libsqlite3-sys` → `bundled`），所以打进二进制的是
`libsqlite3-sys 0.30.1` 自带的 SQLite 3.46.0。RUSTSEC-2026-0459 描述的是：当进程打开一个数据库
文件、而旁边存在页大小与之不同的 `-wal` 兄弟文件时，3.54.0 之前的 SQLite 可能返回未初始化的
page-cache 内存。修复版本是 3.54.0，对应 `libsqlite3-sys 0.39.0`。

**为什么本仓库升不上去**：`sqlx-sqlite 0.8.6` 把 `libsqlite3-sys` 钉在 `^0.30.1`，
`sqlx-sqlite 0.9.0` 也只放宽到 `>=0.30.1, <0.38.0`——**当前没有任何已发布的 sqlx 版本能取到
0.39.0**。因此这一项不是"忘了升级"，而是上游还没有可走的路。

**风险面**：公告的向量是 `AV:L`（本地）且需要 `UI:P`——攻击者必须已经能把构造好的 DB+WAL 文件对
放到 bot 会打开的位置，也就是已经拿到状态目录的写权限。本项目里该目录是容器卷，不对群成员可达。

**复核触发条件**：`sqlx` 发布带 `libsqlite3-sys >=0.39.0` 的版本时，把升级作为独立条目评估
（需要重跑全部迁移兼容性用例）；在那之前不要为了"清掉这条公告"去换 SQLite 链接方式或打补丁分支。

## 这条记录的边界

只覆盖"锁文件里解析到的版本"和"公开公告"，不做可达性证明之外的推断。主版本旧本身不是漏洞结论：
`vite 5` 之于 `vite 8` 只是版本差，真正的判断依据是上表的公告与实际加载路径。
