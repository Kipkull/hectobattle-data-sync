# hectobattle-data-sync

GitHub Actions cron 仓库，绕开腾讯云 IP 段被反爬封禁，从 GH runner 拉取
第三方 lineup 数据 → POST 回 `hectobattle` backend。

## 当前同步源

| 脚本 | 数据源 | Cron | 状态 |
|---|---|---|---|
| `livescore_lineup_sync.py` | livescore.com | `5 * * * *` 每小时 5 分 | ✅ |
| `sofascore_lineup_sync.py` | api.sofascore.com | `35 * * * *` 每小时 35 分 | ✅ |

错峰 30 min, 双源覆盖, 互为 fallback.

## 配置

GitHub Settings → Secrets → Actions → New repository secret：

```
HECTOBATTLE_ADMIN_TOKEN = <hectobattle 后端 ADMIN_TOKEN 值>
```

## 触发

- **自动**：cron 每小时跑
- **手动**：Actions tab → Livescore Lineup Sync → Run workflow

## 架构文档

完整设计见主 repo `Kipkull/hectobattle` 的 `docs/lineup-sync-architecture.md`。

## 维护

修脚本：直接在本仓库 commit + push。下一次 cron 自动用最新脚本。
