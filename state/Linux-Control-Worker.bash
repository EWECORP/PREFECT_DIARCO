# Server
curl -s http://<HOST>:4200/api/health
docker ps --filter "name=prefect"
docker logs --tail=200 <contenedor_server>

# Prefect CLI
prefect version
prefect work-pool ls
prefect worker ls
prefect deployment ls
prefect flow-run ls --limit 20 --state Scheduled

# Worker (systemd)
systemctl status prefect-worker
journalctl -u prefect-worker -n 200 --no-pager
