# ZstdNet Velocity

用于 **Velocity 4.3.0 build 44** 的客户端连接压缩插件，当前版本 **2.1.0**。在正版验证完成后，与配套客户端协商连续 Zstd 传输，数据仍经过原版 AES 加密；不新增监听端口，后端链路保持标准 Minecraft 协议。未安装客户端模组的玩家照常使用原版连接。

本仓库只包含代理插件和公共核心。客户端位于 [sitbat/zstdnet-client](https://github.com/sitbat/zstdnet-client)：

| 客户端游戏版本 | 分支 | 配套客户端版本 |
| --- | --- | --- |
| Fabric 1.21.11 | [main](https://github.com/sitbat/zstdnet-client/tree/main) | 2.1.1 |
| Fabric 26.3 | [mc/26.3](https://github.com/sitbat/zstdnet-client/tree/mc/26.3) | 2.2.1 |

两个分支均使用本插件 2.1.0 和传输协议 v2。Velocity 自身不做跨版本翻译；后端版本不同于客户端实际连接协议时，仍需独立的协议转换器。26.3 客户端与 ViaFabricPlus 配合时使用已修复翻译顺序的客户端 2.2.1。

## 安装与认证

1. 从 [Releases](https://github.com/sitbat/zstdnet-velocity/releases) 下载 `zstdnet-velocity-2.1.0.jar`，停服后放入 Velocity 的 `plugins/`，移除旧版同名插件。代理使用 Java 25。
2. 从 [客户端 Releases](https://github.com/sitbat/zstdnet-client/releases) 下载对应 Minecraft 版本的生产 JAR，放入玩家的 `mods/`。后端不安装这个客户端模组。
3. Velocity 保持 `online-mode=true`、`player-info-forwarding-mode="modern"`，使用现有 forwarding secret。插件沿用代理的正版身份，不修改玩家 UUID、GameProfile 或认证流程。
4. Fabric 后端沿用 FabricProxy-Lite，secret 与 Velocity 一致。现有 `server.properties` 的 `online-mode=true` 可以保留，同时保持 FabricProxy-Lite 的 `hackOnlineMode=true`。这里的后端登录由 FabricProxy-Lite 接管，身份以正版代理和 modern forwarding 为信任边界，并非两次独立的 Mojang 登录握手；后端入口应只允许可信代理访问。
5. 重启后仍连接原代理地址。不要同时启用另一套 ZstdNet 服务端压缩协商。

插件固定针对上述 Velocity 构建及其内部管线，其他构建不作兼容承诺。JAR 包含 Zstd JNI 的平台原生库，包括 macOS ARM64。UDP 和语音端口沿用原有配置；PROXY protocol 使用 Velocity 自身功能。

## 配置

首次运行生成 `plugins/zstdnet-proxy/zstdnet.properties`：

```properties
enabled=true
level=9
debug=false
max_conn_per_ip=64
max_req_per_window=50
request_window=10s
ban_duration=1m
report_admin_uuids=
```

`level` 为代理下行压缩等级，范围 1–22，历史窗口上限为 8 MiB；现有配置值会保留。客户端自行配置上行等级。请求限制在 PreLogin 阶段生效，并发限制统计已登录的真实代理连接；对应非正数可关闭限制。除 `allowreport` 命令写入的名单外，修改配置后重启生效。

`report_admin_uuids` 是逗号分隔的正版 UUID 白名单，默认空。Fabric 后端的 OP 权限不会自动成为 Velocity 权限。

## 命令与权限

| 代理命令 | 用途 | 权限 |
| --- | --- | --- |
| `/zstdnetproxy status` | 状态、压缩连接数与等级 | `zstdnet.admin` |
| `/zstdnetproxy statistics` | 本次运行累计上下行统计 | `zstdnet.admin` |
| `/zstdnetproxy report [范围]` | 将报告 JSON 导出到插件数据目录的 `reports/` | `zstdnet.report`、`zstdnet.admin` 或 UUID 白名单 |
| `/zstdnetproxy export [范围]` | `report` 的别名 | 同上 |
| `/zstdnetproxy allowreport <在线玩家名>` | 保存该玩家已验证的真实 UUID，立即授权报告 | `zstdnet.admin` |

Velocity 控制台可管理和导出，输入时省略 `/`。未安装权限插件时，可在控制台执行 `zstdnetproxy allowreport 玩家名`。普通玩家默认不能读取代理全局历史。

报告范围为 `today`、`session`、`24h`、`7d`、`30d`，默认 `today`，按代理系统时区计算；`session` 指本次代理运行。日统计、5 分钟桶和运行记录持久保存。客户端使用 `/zstdreport [范围]` 下载并生成本地 HTML，每次请求至少间隔 15 秒。

代理每秒向压缩连接发送全局 HUD 摘要；客户端 HUD 默认关闭，由 `/zstdhud on|off|toggle` 控制。全局统计仅覆盖本插件的压缩连接，不含普通连接、后端链路或 UDP。`raw` 为进入 Zstd 的 Minecraft 帧，`wire` 包含 Zstd 帧头、不含 TCP/IP 头及协商前流量；`wire/raw` 越低，节省越多。

## 构建

需要 **JDK 25 和 JDK 21**：JDK 25 运行 Gradle 9.6.0、编译与测试 Velocity 模块，公共核心使用 Java 21 工具链。设置 `JAVA25_HOME`、`JAVA21_HOME` 后，在 PowerShell 执行：

```powershell
$env:JAVA_HOME = $env:JAVA25_HOME
./tools/download-velocity.ps1
./gradlew.bat :common:test :velocity:test :velocity:jar
```

Linux/macOS 可使用 `pwsh ./tools/download-velocity.ps1` 和 `./gradlew`。下载工具从 PaperMC 官方获取固定目标，并验证 SHA256；Gradle Wrapper 也固定发行版校验值。目标代理 JAR 只作为编译和测试依赖，不打包进插件，`runtime/` 不入库。

产物为 `velocity/build/libs/zstdnet-velocity-2.1.0.jar`。测试范围和验证边界见 [VALIDATION.md](VALIDATION.md)。

## 来源与许可

[MIT License](LICENSE)，保留原始版权通知。连续 Zstd 帧、流量统计和报告资源参考或复用自 [wish131400/zstdnet](https://github.com/wish131400/zstdnet)，基准提交 `0b38e7c3e65a445ce9b8ed94c19d3badc9b92b88`。本项目将其适配到正版 Velocity 前端连接，并补充生命周期、输入边界和报告权限处理。
