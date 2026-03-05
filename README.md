# LAVA Event Listener

A Python service that connects to LAVA CI server instances via websockets, monitors device health changes, and automatically creates/updates Jira Service Management tickets.

- **Bad / Maintenance / Retired** health → creates a Jira ticket
- **Good** health → adds a recovery comment to the existing ticket
- Connects to multiple LAVA servers concurrently
- Reconnects automatically with exponential backoff
- Optional BetterStack heartbeat monitoring and Sentry error tracking

## Deployment on AWS Lightsail

### 1. Create a Lightsail instance

- Image: **Ubuntu 24.04 LTS**
- Plan: **$3.50/mo** (512 MB RAM, 1 vCPU) is sufficient
- Enable the **Static IP** option so the address doesn't change on reboot

### 2. SSH into the instance and install dependencies

```bash
sudo apt update && sudo apt install -y python3 python3-venv git
```

### 3. Clone the repository

```bash
sudo mkdir -p /opt/lava-event-listener
sudo chown $USER:$USER /opt/lava-event-listener
git clone <YOUR_REPO_URL> /opt/lava-event-listener
```

Or copy the files manually with `scp`:

```bash
scp -r ./* user@<LIGHTSAIL_IP>:/opt/lava-event-listener/
```

### 4. Create a Python virtual environment

```bash
cd /opt/lava-event-listener
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 5. Create the configuration file

```bash
sudo mkdir -p /etc/lava-event-listener
sudo cp config.yaml.example /etc/lava-event-listener/config.yaml
sudo chmod 600 /etc/lava-event-listener/config.yaml
sudo nano /etc/lava-event-listener/config.yaml
```

Edit the config with your actual values:

- `lava_servers` — your LAVA server URLs (and optional credentials)
- `jira.url` — your Jira Cloud URL (e.g. `https://yourorg.atlassian.net`)
- `jira.email` — the email associated with the API token
- `jira.api_token` — generate one at https://id.atlassian.com/manage-profile/security/api-tokens
- `jira.project_key` — the Jira project key (e.g. `LAVAOPS`)
- `jira.issue_type` — the issue type to create (e.g. `Service Request`)
- `sentry.dsn` — (optional) your Sentry DSN
- `betterstack.heartbeat_url` — (optional) your BetterStack heartbeat URL

### 6. Create a service user

```bash
sudo useradd --system --no-create-home --shell /usr/sbin/nologin lava-listener
```

### 7. Set up the state directory

```bash
sudo mkdir -p /var/lib/lava-event-listener
sudo chown lava-listener:lava-listener /var/lib/lava-event-listener
```

Make sure the `state_file` in your config points here:

```yaml
state_file: "/var/lib/lava-event-listener/state.json"
```

### 8. Give the service user read access to the config

```bash
sudo chown root:lava-listener /etc/lava-event-listener/config.yaml
sudo chmod 640 /etc/lava-event-listener/config.yaml
```

### 9. Install the systemd service

Update the `ExecStart` path in the unit file to use the venv Python:

```bash
sudo cp lava-event-listener.service /etc/systemd/system/
sudo sed -i 's|/usr/bin/python3|/opt/lava-event-listener/.venv/bin/python|' /etc/systemd/system/lava-event-listener.service
sudo systemctl daemon-reload
```

### 10. Start the service

```bash
sudo systemctl enable lava-event-listener
sudo systemctl start lava-event-listener
```

### 11. Verify it's running

```bash
sudo systemctl status lava-event-listener
sudo journalctl -u lava-event-listener -f
```

You should see log output like:

```
INFO  [__main__] Starting LAVA Event Listener with 1 server(s): linaro-production
INFO  [lava_event_listener.listener] [linaro-production] Connecting to wss://validation.linaro.org/ws/
INFO  [lava_event_listener.listener] [linaro-production] Connected.
```

## Managing the service

```bash
# View live logs
sudo journalctl -u lava-event-listener -f

# Restart after config changes
sudo systemctl restart lava-event-listener

# Stop the service
sudo systemctl stop lava-event-listener

# Check the current state file
cat /var/lib/lava-event-listener/state.json
```

## Running manually (for development/testing)

```bash
cd /opt/lava-event-listener
source .venv/bin/activate
python -m lava_event_listener.main -c config.yaml
```
