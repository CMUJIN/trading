# Futures Chip Analysis (GitHub Actions + Notion)

### What it does
- Twice daily auto-runs at **Beijing 16:00 / 02:00**
- Per-symbol **start dates** in `config.yaml`
- Outputs to `docs/{SYMBOL}/SYMBOL_latest.png` and dated files
- Optional **Notion sync** with auto-create DB

### Setup
1. Create a new GitHub repo and upload all files.
2. **Settings → Pages**: Source = Deploy from branch, Branch = `main` and `/docs`.
3. **Settings → Secrets → Actions**:
   - `NOTION_TOKEN` (required for Notion sync)
   - `NOTION_DB` (optional; leave empty to auto-create)
   - `NOTION_PARENT_PAGE` (required if NOTION_DB is empty)
   - `PAGES_BASE` (e.g., `https://USERNAME.github.io/REPO`)
4. Actions will run at 08:00/18:00 UTC.

### Config (`config.yaml`)
```yaml
symbols:
  - code: JM2601
    start: 2025-09-03
  - code: M2601
    start: 2025-09-03
freq: "60m"
```

### Notion schema (auto-created if needed)
- **品种** (Title)
- **日期** (Date)
- **图表链接** (URL)
- **备注** (Rich text)
