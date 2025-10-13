# Futures Chip Analysis (GitHub Actions + Notion)

- Twice daily auto-runs at **Beijing 16:00 / 02:00**
- Per-symbol **start dates** in `config.yaml`
- Outputs:
  - `docs/{SYMBOL}/{SYMBOL}_chipzones_hybrid.png`
  - `docs/{SYMBOL}/{SYMBOL}_chipzones_hybrid.csv`
- Chinese font installed in CI to avoid garbled text
- Optional **Notion sync**: auto-create DB (Title 品种 / Date 日期 / Url 图表链接 + all CSV columns)

## Setup
1. Upload files to GitHub.
2. Settings → Pages: Branch=`main`, Folder=`/docs`.
3. Secrets:
   - `NOTION_TOKEN` (required for sync)
   - `NOTION_DB` (optional; leave empty to auto-create)
   - `NOTION_PARENT_PAGE` (required if DB is empty)
   - `PAGES_BASE` (e.g., `https://USERNAME.github.io/REPO`)

## Links
- Image: `https://USERNAME.github.io/REPO/JM2601/JM2601_chipzones_hybrid.png`
- CSV:   `https://USERNAME.github.io/REPO/JM2601/JM2601_chipzones_hybrid.csv`
