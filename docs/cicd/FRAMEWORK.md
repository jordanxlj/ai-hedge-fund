# CI/CD 框架设计（多仓、版本、质量门禁、单板刷写、HIL 测试、Unity 可视化、性能优化）

> 目标：给“软件 + 固件/单板 + 可视化验证（Unity）”这类系统，设计一套可规模化、可审计、可复用、可持续优化的 CI/CD 框架。
>
> 本文默认你使用 GitHub（也适用于 GitLab/Jenkins：核心思想与组件不变，语法不同）。

---

## 总体架构（推荐落地形态）

- **代码托管**：GitHub / GitLab
- **CI 编排**：GitHub Actions（SaaS）+ **自托管 Runner（实验室）**
- **制品库**：GitHub Releases/Packages（轻量）或 Nexus/Artifactory（企业级）或 S3（对象存储）
- **质量平台**：SonarQube（复杂度/重复率/覆盖率/质量门禁）+ CodeQL（安全）+ 语言侧 lint/format
- **HIL/单板控制**：`labgrid`（强烈推荐）/ 自研控制服务（含锁、刷写、电源控制、串口日志）
- **测试框架**：pytest（驱动单板 + 数据采集 + 性能阈值断言）/ GoogleTest（C/C++）等
- **可视化验证**：Unity Test Runner（BatchMode）+ 截图/视频 + 像素 diff（回归）
- **可观测性**：Allure/TestRail（报告）+ Prometheus/InfluxDB + Grafana（趋势、门禁、耗时优化）

> 关键点：**把“能在云上跑的”与“必须在实验室跑的（单板/Unity/硬件）”拆成两条流水线**，通过制品（artifact）与版本清单（manifest）衔接。

---

## 需求映射到流水线分层

你提出的 7 点需求，建议拆成 4 层流水线（从快到慢，从便宜到贵）：

1. **PR 快速门禁（分钟级）**：编译/单测/基础 lint（尽量不依赖外部网络与真实硬件）
2. **主干集成（十分钟级）**：覆盖率/静态扫描/制品产出（可重放、可追溯）
3. **实验室 HIL（小时级，可并行）**：刷写 + 板级测试 + 性能指标（FPS/耗时/资源）
4. **发布/交付（可控频率）**：打标签、生成版本号、发布制品、生成 SBOM/签名、回滚策略

---

## 1) 多仓的版本编译（Multi-repo Build）

### 推荐的 3 种模式（按成熟度从低到高）

- **模式 A：CI 里同时 checkout 多个仓库（快速起步）**
  - 在一个“集成流水线”里 `checkout` 多个 repo，按“版本清单”或固定 tag 构建
  - 适合：团队小、依赖关系清晰、变更不频繁
  - 风险：构建可重放性依赖于你是否严格 pin 版本

- **模式 B：Manifest 仓库（推荐）**
  - 单独建一个 `manifest` 仓库，维护每个组件的 `repo + tag/commit`
  - CI 读取 manifest 拉取对应版本进行集成构建、刷写与测试
  - 优点：**可重放、可审计**，支持“同一个 manifest 复现现场问题”

- **模式 C：Release Orchestration（企业级）**
  - 用 `release-please`（GitHub）或自研发布编排服务，基于依赖拓扑自动决定发布顺序与版本 bump
  - 搭配制品库与签名，实现端到端可追溯交付

### 工具建议

- **GitHub**：`workflow_call`（可复用工作流）+ `repository_dispatch`（跨仓触发）+ `release-please`（多仓/manifest）
- **GitLab**：multi-project pipelines（跨仓触发）+ includes（可复用）
- **Jenkins**：Pipeline + Shared Library（强大但维护成本更高）

---

## 2) 打标签、版本号（Tagging / Versioning）

### 建议规范

- **版本号**：SemVer（`MAJOR.MINOR.PATCH`）+ 可选 build metadata（如 `+sha.abcdef`）
- **分支策略**：Trunk-based（推荐）或 GitFlow（更重）
- **提交规范**：Conventional Commits（让自动生成版本号成为可能）

### 推荐自动化方案（GitHub）

- **`release-please`**：根据提交信息自动生成 release PR、打 tag、生成 changelog
- **签名与溯源**：SLSA provenance（构建来源证明）+ SBOM（CycloneDX/SPDX）

### 正确性验证

- 在测试仓库/测试分支跑一次 release-please：确认
  - tag 命名正确（如 `v1.2.3`）
  - release notes 生成正确
  - 制品与 tag 一一对应、可下载复现

---

## 3) 静态检查（复杂度/metrics 等）

### 推荐“组合拳”

- **基础 lint/format**：黑白名单规则，PR 阶段必须过
- **复杂度 & 代码度量**：
  - Python：`radon`（圈复杂度、MI）、`lizard`（复杂度/函数长度）、`pylint`（可选）
  - C/C++：`lizard`、`clang-tidy`、`cppcheck`、include-what-you-use（可选）
  - TS/JS：`eslint`、`tsc --noEmit`
- **质量门禁平台**：SonarQube（聚合复杂度、覆盖率、重复率、热点）
- **安全**：CodeQL / SAST（建议主干与发布必跑）

### 质量门禁策略（推荐分级）

- **PR 必须通过**：编译、单测、lint、基础复杂度阈值（避免新增劣化）
- **主干/夜间**：SonarQube 全量扫描 + 趋势图
- **发布**：安全扫描 + SBOM + 签名

### 正确性验证

- 在 CI 里输出机器可读报告（JSON/XML），并把关键结论写入 Job Summary
- 确认“门禁失败的原因”可定位到具体文件/函数（避免只给一个分数）

---

## 4) 实际环境（单板）的烧录刷写（Flashing）

### 推荐架构：自托管 Runner + 设备资源管理

单板刷写与测试 **不建议** 放在纯云 runner：你需要 USB/JTAG、串口、摄像头/采集卡、电源控制等硬件资源。

- **自托管 Runner 放置**：实验室机柜/工位主机（Linux 优先）
- **Runner 标签**：例如 `self-hosted`, `lab`, `board-a`, `board-b`
- **设备互斥**：必须有“锁”，避免两个 Job 同时抢同一块板
  - 推荐：`labgrid` 自带 place/lock
  - 备选：自研“设备分配服务”（Redis/DB + lease）

### 刷写链路（通用步骤）

1. **获取待测制品**：从 CI 制品库下载（固件 bin/elf、rootfs、配置包等）
2. **进入可刷写态**：复位、拉 BOOT 引脚、进入 DFU/fastboot/JTAG 模式
3. **刷写**：`openocd` / `dfu-util` / `fastboot` / 厂商工具
4. **刷写后验证**：
   - 读版本号（串口/网络 API/设备信息区）
   - 关键分区校验（hash/签名）
5. **采集日志**：全程抓串口 log、刷写 log、dmesg（必要时抓功耗/温度）

### 正确性验证（刷写阶段）

- **可重复性**：同一个制品 + 同一套脚本，在不同 runner 上刷写结果一致
- **可追溯性**：刷写记录能关联到 `repo@sha/tag` + 制品 checksum + 板卡 ID + 时间
- **可诊断性**：失败能定位到“进入刷写态/下载制品/刷写/启动/健康检查”的哪一步

---

## 5) 单板环境的测试执行（例如帧率/性能数据）

### 推荐做法：把“板级测试”当成一个标准测试套件

- **测试驱动**：pytest（Python）很适合写“设备控制 + 断言 + 指标采集”
- **标记与分层**：
  - `smoke`：几分钟内完成（PR 合并前可选跑）
  - `nightly`：全量回归（夜间）
  - `perf`：性能基准（对比历史趋势）
- **结果产物**：
  - JUnit XML（CI 识别）
  - Allure 原始数据（生成报告）
  - 指标数据（FPS/延时/CPU/GPU/内存）推送到时序库（InfluxDB/Prometheus remote write）

### 帧率/耗时这类指标怎么“门禁”

- **阈值门禁**：例如 FPS 必须 ≥ 55，启动耗时 ≤ 3.0s
- **回归门禁**（更推荐）：相对基线退化不得超过 X%
  - 例如：`p95 延时` 相对 `main` 最近 7 天均值退化不得超过 5%

### 正确性验证（板测阶段）

- **环境一致性**：锁定板卡硬件版本、固件配置、温度区间；必要时用恒温箱
- **测量稳定性**：同一版本重复跑 3 次，方差在可接受范围
- **数据可信**：原始日志可回放；指标计算脚本有单测；异常值有剔除规则与告警

---

## 6) 可视化验证（例如 Unity 上触发功能）

### 两条常见路线

- **路线 A：Unity 自动化测试（推荐）**
  - Unity Test Runner（EditMode/PlayMode），CI 用 BatchMode 运行
  - 输出 XML/HTML 报告 + 截图/视频
  - 适合：UI/交互逻辑可自动化、回归稳定

- **路线 B：外部系统触发 Unity 行为 + 采集结果**
  - CI 通过 RPC/网络命令触发 Unity 场景/功能
  - 采集：屏幕截图、RenderDoc capture、视频流帧
  - 对比：golden image 像素 diff 或特征点匹配（避免噪声）

### 正确性验证（可视化阶段）

- **确定性**：同一版本结果一致（尽量关闭随机性、固定帧率、固定分辨率/渲染设置）
- **抗噪**：对比策略要能容忍轻微抖动（阈值、遮罩区域、结构相似度 SSIM）
- **可审计**：失败时能直接看到“期望 vs 实际”的截图差异（diff 图）

---

## 7) 总体集成性能（流水线耗时）改进与提升

### 先度量，再优化（强烈建议）

- **流水线耗时拆解**：checkout / restore cache / build / test / upload artifacts / HIL queue time
- **数据落库与看板**：
  - GitHub Actions：可用 job duration + 自定义 step 计时
  - 自建：Prometheus/InfluxDB + Grafana（按 repo/分支/runner/板卡维度聚合）

### 常用优化手段（命中率最高的前 8 个）

- **依赖缓存**：pip/poetry、pnpm、ccache/sccache（固件/原生编译）
- **增量与分层构建**：Docker multi-stage + buildx cache；或 Bazel 远程缓存
- **并行化**：matrix（多平台/多板卡）、拆分测试套件（smoke vs full）
- **减少外部依赖**：镜像仓库/依赖代理、内网 PyPI/NPM 镜像
- **制品复用**：CI 构建一次，HIL/Unity 直接下载同一制品，不重复编译
- **自托管 runner 规格**：CPU/IO/网络瓶颈常见；必要时用裸金属
- **队列治理**：板卡池扩容、优先级队列、夜间跑重任务
- **门禁策略分级**：PR 只跑最关键的快检查，重检查放主干/夜间

### 正确性验证（性能优化）

- 优化前后对比同一时间窗口数据（至少一周），避免“偶然更快”
- 保证缓存命中不会引入“脏构建”（缓存 key 必须包含关键输入：锁文件、编译选项、工具链版本）

---

## 落地清单（你可以按这个顺序实施）

1. **确定版本策略**：SemVer + Conventional Commits（是否多仓 manifest）
2. **建立可复用 CI 模板**：build/test/quality（PR 必跑）
3. **接入 SonarQube + CodeQL**：先报表后门禁
4. **建设实验室 Runner 与板卡池**：锁、刷写、串口、电源、采集
5. **把板级测试产品化**：pytest 套件 + 报告 + 指标看板
6. **Unity 自动化**：BatchMode + 截图 diff + 报告
7. **耗时治理**：看板 + 缓存/并行/制品复用 + 持续迭代

