# dews-personal-portfolio
Documenting my journey into data engineering through small projects, experiments, and continuous learning ehehe🙂‍↕️

## What's in here

- `index.html`, `assets/` — the portfolio website (static, no build step).
- `portfolio-blueprint.html` — the working content package: positioning, 7 project case studies, the Canva PDF page-by-page plan, and website copy, all sourced from the CV/competition paper.
- `.github/workflows/deploy.yml` — deploys `index.html` to GitHub Pages on every push to `main`.

## Deploying

This repo deploys via GitHub Actions + GitHub Pages. One manual step is required once, since it can't be done from the CLI:

1. Push this repo to GitHub (already set up if you're reading this from the repo).
2. On GitHub: **Settings → Pages → Build and deployment → Source → GitHub Actions**.
3. Push to `main` (or re-run the "Deploy portfolio to GitHub Pages" workflow from the Actions tab) — the site publishes to `https://<username>.github.io/dews-personal-portfolio/`.
4. Once ready, point the custom domain (`rayhanendra.com`) at the Pages site under **Settings → Pages → Custom domain**, and add the matching DNS records at your domain registrar.
