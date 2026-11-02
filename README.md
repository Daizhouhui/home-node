# VLESS over WebSocket with SSTP

This repository contains a Cloudflare Worker and a GitHub Actions job that refreshes a public list of VPN Gate TCP SSTP endpoints. The job updates the GitHub repository only; it does not deploy or upload anything to Cloudflare.

## 1. Use a public node-list repository

The Worker must be able to fetch the list without a GitHub token, so the repository containing `generated/sstp-nodes.txt` must be public. The generated list contains public VPN Gate hostnames and ports only; it does not contain your VLESS UUID.

If your existing project repository contains private files or credentials, do not make that repository public. Create a small, separate public repository for `vpngate.py` and `.github/workflows/update-sstp-nodes.yml`; keep the Worker source and its secrets private.

## 2. Generate the endpoint list

The workflow `.github/workflows/update-sstp-nodes.yml` runs every six hours. To run it immediately:

1. Open the repository's **Actions** tab.
2. Select **Update SSTP node list**.
3. Select **Run workflow** on the default branch.
4. Wait for the run to finish. The workflow commits the list to `generated/sstp-nodes.txt`.

The same file can also be generated locally with Python 3:

```powershell
python vpngate.py
```

The generator uses only Python's standard library. If both VPN Gate data sources fail or return no TCP SSTP nodes, it exits with an error and does not replace the previous list.

## 3. Configure the Worker manually

Copy `sstp.js` into your Cloudflare Worker using the dashboard or the web interface you prefer. This repository does not run Wrangler or call the Cloudflare deployment API.

Set these Worker variables:

| Variable | Value |
| --- | --- |
| `UUID` | Your VLESS UUID |
| `SSTP_NODES_URL` | `https://raw.githubusercontent.com/OWNER/REPOSITORY/main/generated/sstp-nodes.txt` |

Replace `OWNER/REPOSITORY` with the public GitHub repository path and `main` with its default branch if different. SSTP credentials default to `vpn` / `vpn`, which are the standard VPN Gate credentials. If your SSTP server uses different credentials, set `SSTP_USERNAME` and `SSTP_PASSWORD` in the Worker.

Do not set `SSTP_PROXY` when using `SSTP_NODES_URL`; `SSTP_PROXY` selects one fixed server instead of the generated pool.

## 4. Connect a client

Create one VLESS node using your Worker hostname:

- Protocol: VLESS
- Address: your Worker hostname
- Port: 443
- UUID: the same value configured as the Worker `UUID`
- Encryption: none
- Transport: WebSocket
- TLS: enabled
- SNI and WebSocket Host: your Worker hostname
- WebSocket path: `/`

The UUID belongs in the client configuration as well as the Worker environment. It is intentionally not written into the generated SSTP endpoint list.

## Notes

- The Worker caches the endpoint list for up to five minutes and tries up to five randomly ordered endpoints for a connection.
- This Worker currently supports VLESS TCP over WebSocket and IPv4 destinations. It does not support UDP or IPv6 destinations through SSTP.
- VPN Gate endpoints are public and can change or stop working. The Worker tries alternatives, but a generated endpoint is not a guarantee of availability.
- Keep Worker credentials and any non-default SSTP passwords in Cloudflare secrets, not in this repository.
