# PowerTrader AI — Docker Install Guide

---

## 1. Install Docker Desktop

- **Mac / Windows:** https://www.docker.com/products/docker-desktop/
- **Linux:** https://docs.docker.com/desktop/install/linux/

Open Docker Desktop and wait for it to finish starting.

---

## 2. Create Your Data Folder

Create a folder on your computer — this is where PowerTrader stores all its data.

| Platform | Suggested path |
|----------|---------------|
| Mac | `/Users/yourname/powertrader` |
| Windows | `C:\Users\yourname\powertrader` |
| Linux | `/home/yourname/powertrader` |

Inside that folder, create two files using a text editor:

### `exchange_api_keys.json`
Your exchange API credentials. Only include the exchanges you use:

```json
{
  "kraken": {
    "api_key": "YOUR_KEY",
    "api_secret": "YOUR_SECRET"
  }
}
```

> **Mac:** Open TextEdit → Format → Make Plain Text → save as `exchange_api_keys.json`  
> **Windows:** Open Notepad → Save As → set type to *All Files* → name it `exchange_api_keys.json`

### `pt_config.json`
Create this file containing just `{}`. The app writes your settings here when you use the web UI.

---

## 3. Download and Edit `docker-compose.yml`

`docker-compose.yml` is the single configuration file that tells Docker where your data folder is and which port to use. You only need to edit it once.

**Mac / Linux** — open Terminal:
```bash
curl -o docker-compose.yml https://raw.githubusercontent.com/swedishhh/PowerTrader_AI/main/docker-compose.yml
```

**Windows** — open PowerShell:
```powershell
Invoke-WebRequest -Uri https://raw.githubusercontent.com/swedishhh/PowerTrader_AI/main/docker-compose.yml -OutFile docker-compose.yml
```

Open `docker-compose.yml` in a text editor and:

- Replace `/Users/yourname/powertrader` **(three places)** with the full path to your data folder
- Change `8080` on the left side of the port line if that port is already in use on your machine

---

## 4. Pull and Run

Open a terminal in the folder containing `docker-compose.yml` and run:

**Mac / Linux:**
```bash
docker compose pull && docker compose up -d
```

**Windows (PowerShell):**
```powershell
docker compose pull; docker compose up -d
```

Then open **http://localhost:8080** in your browser.

---

## Alternative for Windows: run inside WSL2 (no Docker Desktop)

Same image, same compose file, but Docker runs inside a Linux (WSL2) environment on your Windows PC. Choose this if you want PowerTrader to start with Windows without logging in to Docker Desktop, or if you run other Linux services alongside it.

### 1. Install WSL2 and Docker Engine

In PowerShell (administrator): `wsl --install -d Ubuntu-24.04`, then reboot and open the Ubuntu terminal. Inside it:

```bash
# Run systemd so Docker starts automatically with the distro
printf '[boot]\nsystemd=true\n' | sudo tee /etc/wsl.conf
# Docker Engine (official convenience script)
curl -fsSL https://get.docker.com | sudo sh
sudo usermod -aG docker $USER
sudo systemctl enable docker
```

Then run `wsl --shutdown` in PowerShell and reopen Ubuntu. `docker compose version` should now work.

### 2. Put your data folder on the Linux filesystem

Create it **inside WSL** (e.g. `~/powertrader`), not under `/mnt/c/...`. The database files are memory-mapped, which is slow and can be unreliable across the Windows/Linux boundary.

```bash
mkdir -p ~/powertrader/state && cd ~/powertrader
echo '{}' > pt_config.json
nano exchange_api_keys.json     # same contents as in step 2 above
```

### 3. Create `docker-compose.yml` in that folder

Paths are relative, so the folder holding this file *is* your data folder:

```yaml
services:
  powertrader:
    image: swedishhh/powertrader:latest
    platform: linux/amd64
    ports:
      - "8080:8080"             # change the left side if 8080 is taken
    volumes:
      - ./state:/app/state
      - ./pt_config.json:/app/pt_config.json
      - ./exchange_api_keys.json:/app/exchange_api_keys.json
    stop_grace_period: 60s      # lets the trainer close its database cleanly
    restart: unless-stopped
```

### 4. Run

```bash
docker compose pull && docker compose up -d
```

Open **http://localhost:8080** in your Windows browser. `restart: unless-stopped` plus Docker enabled at boot brings it back after a reboot, as long as the WSL distro is running (see below).

### Starting at Windows boot

WSL only runs while something keeps it alive. Create a scheduled task that starts it at boot (PowerShell, administrator):

```powershell
$a = New-ScheduledTaskAction -Execute "wsl.exe" -Argument "-d Ubuntu-24.04 -- sleep infinity"
$t = New-ScheduledTaskTrigger -AtStartup
Register-ScheduledTask -TaskName "Start WSL" -Action $a -Trigger $t -User $env:USERNAME -RunLevel Highest
```

### Reaching it from other devices on your network

By default the UI is only reachable from the PC itself. Add `networkingMode=mirrored` under `[wsl2]` in `%UserProfile%\.wslconfig`, run `wsl --shutdown`, and allow the port in Windows Firewall. There is no login on the UI, so only do this on a network you trust.

### Moving from Docker Desktop to WSL

Stop the Docker Desktop container first (**never run both against the same data and API keys**), set its restart policy to `no` (`docker update --restart=no <container>`), copy the data folder into WSL (`cp -a /mnt/c/.../powertrader ~/powertrader`), delete any `*:Zone.Identifier` / `*:mshield` files the copy produced, then continue from step 3.

---

## Managing the Container

In Docker Desktop → **Containers** you can start, stop, view logs, and open the UI via the port link.

---

## Updating

Run the same one-liner from the folder containing `docker-compose.yml`:

**Mac / Linux:**
```bash
docker compose pull && docker compose up -d
```

**Windows (PowerShell):**
```powershell
docker compose pull; docker compose up -d
```

Your data folder is never touched by updates.

---

## Troubleshooting

**Port 8080 already in use** — change the left side of the port line in `docker-compose.yml` (e.g. `9090:8080`) and access on `http://localhost:9090`.

**Starts in demo mode** — check `exchange_api_keys.json` is valid JSON, then restart: `docker compose restart`.

**Container exits immediately** — Docker Desktop → Containers → click the container name → Logs tab.

---

## For Maintainers — Publishing a New Image

```bash
# From the repo root — CACHEBUST forces a fresh git clone every build
docker build --build-arg CACHEBUST=$(date +%s) -t swedishhh/powertrader:latest . \
&& docker push swedishhh/powertrader:latest
```

To also publish a versioned tag (e.g. `2.0.0`), add `-t` for each tag and push both:

```bash
DESC="Fixes BTC trainer segfault — get_patterns_matrix incremental rewrite. Other fixes."
REV=$(git ls-remote https://github.com/swedishhh/PowerTrader_AI.git HEAD | cut -f1)
VERSION="4.0.2"
docker build \
--build-arg CACHEBUST=$(date +%s) \
--label "org.opencontainers.image.description=$DESC" \
--label "org.opencontainers.image.revision=$REV" \
-t swedishhh/powertrader:latest \
-t swedishhh/powertrader:$VERSION . \
&& docker push swedishhh/powertrader:latest \
&& docker push swedishhh/powertrader:$VERSION
```
