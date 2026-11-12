# OSDU-Lite on two Raspberry Pi 5 nodes

This is a minimal, private-LAN demo setup for **two Raspberry Pi 5 boards with
16 GB RAM each**, using Docker Engine and K3s (lightweight Kubernetes). No Helm,
container registry account, Java, or locally built image is required.

| Node | Example address | Runs |
| --- | --- | --- |
| `pi1` | `192.168.1.101` | K3s server/control plane and one OSDU-Lite API pod |
| `pi2` | `192.168.1.102` | K3s agent/worker and one OpenSearch pod |

K3s normally uses containerd. Here, `--docker` makes it use Docker through its
bundled `cri-dockerd` integration on **both** nodes. Do not also start the Compose
stack: Kubernetes will manage the application containers.

**Limitations:** this is not production OSDU or a highly available cluster.
One control plane and one search node are intentional. Storage, uploaded files,
and other API state are in memory. An API restart resets its managed search
index and reloads the 15 sample records. OpenSearch below also uses temporary
storage; adding a persistent volume would not make the API durable. **Never
scale the API above one replica** against this shared search index.

The instructions are based on the repository's Docker/Compose configuration and
upstream installation documentation; they have not been hardware-tested on this
two-Pi setup.

## 1. Prepare both Pis

Use Raspberry Pi Imager to install **Ubuntu Server 24.04 LTS, 64-bit, for
Raspberry Pi** on each board. This guide assumes fresh installations, not nodes
already running other clusters or Docker workloads.

- Use reliable power supplies and active cooling.
- Prefer SSD boot/storage with at least 32 GB free on each Pi; microSD is slower
  and wears out more quickly.
- Connect both Pis to the same trusted wired LAN with Internet access.
- In Imager, enable SSH, configure your own username and SSH key, and set unique
  hostnames `pi1` and `pi2`.
- Reserve both addresses in your router's DHCP settings. Replace the example
  addresses throughout this guide with your actual addresses.

All command blocks below run in **Bash on the named Pi**, over SSH, unless noted
otherwise. Linux paths in these commands refer to the Pis, not your workstation.

On **each Pi**, connect using the account you configured:

```bash
# From your workstation; change the address for pi2.
ssh YOUR_USER@192.168.1.101
```

Then, on **both Pis**:

```bash
sudo apt-get update
sudo apt-get upgrade -y
sudo apt-get install -y ca-certificates curl git python3
uname -m
hostname
```

`uname -m` must report `aarch64`. Do not use a 32-bit OS. Confirm each hostname
is correct; if necessary, run `sudo hostnamectl set-hostname pi1` on the first
Pi and `sudo hostnamectl set-hostname pi2` on the second.

Disable swap for this simple Kubernetes configuration:

```bash
sudo swapoff -a
sudo cp -n /etc/fstab /etc/fstab.before-k3s
sudo sed -i '/^[^#].*[[:space:]]swap[[:space:]]/s/^/#/' /etc/fstab
sudo reboot
```

Reconnect after reboot and check on **both Pis**:

```bash
swapon --show
grep -w memory /proc/cgroups
```

`swapon --show` should be empty. If swap returns, disable the service creating
that swap device before continuing. In `/proc/cgroups`, the `memory` row's last
column must be `1`; see troubleshooting if it is not.

### Network access

Keep the cluster behind your router; **do not port-forward it to the Internet**.
If you have host/network firewalls, allow these connections explicitly:

| Traffic | Allowed source and destination |
| --- | --- |
| TCP 22 | Your administration workstation to both Pis |
| TCP 6443 | `pi2` to `pi1` for the Kubernetes API |
| UDP 8472 | Between the two Pis for Flannel VXLAN; never from the Internet |
| TCP 10250 | Between the two Pis for kubelet/metrics |
| TCP 30089 | Trusted LAN clients to the Pis for the demo API |

Both nodes need outbound DNS and HTTPS for installation and image downloads.
The default K3s pod/service ranges are `10.42.0.0/16` and `10.43.0.0/16`; they
must not overlap your LAN or VPN. Firewalls must also permit pod/service traffic
on these ranges. Do not assume UFW alone protects Docker or Kubernetes published
ports; restrict access at your LAN/router boundary too.

## 2. Install Docker on both Pis

On **both Pis**, use Docker's convenience installer, suitable for this fresh
development setup. Download and inspect it before running:

```bash
curl -fsSL https://get.docker.com -o get-docker.sh
less get-docker.sh
sudo sh get-docker.sh
sudo systemctl enable --now docker
sudo docker run --rm hello-world
sudo docker version
```

Press `q` to exit `less`. Continue only if `hello-world` succeeds. Using `sudo`
avoids adding your user to the root-equivalent Docker group.

## 3. Create the Kubernetes control plane on pi1

On **pi1**:

```bash
curl -fsSL https://get.k3s.io -o install-k3s.sh
less install-k3s.sh
sudo sh install-k3s.sh server \
  --docker \
  --node-name pi1 \
  --node-ip 192.168.1.101 \
  --disable traefik \
  --disable servicelb

sudo k3s kubectl wait --for=condition=Ready node/pi1 --timeout=180s
sudo k3s kubectl get nodes -o wide
sudo k3s --version
sudo cat /var/lib/rancher/k3s/server/node-token
```

The default single-server SQLite datastore is sufficient. K3s allows application
pods on the server node, so no control-plane taint removal is needed.

Record the exact version from the first line of `k3s --version`, such as
`vX.Y.Z+k3sN`, and the join token. **Keep the token private**: it grants cluster
access. Do not commit it or the administrator kubeconfig to the repository.

## 4. Join pi2 as the worker

On **pi2**, enter the exact version and token printed on pi1 when prompted:

```bash
read -r -p "K3s version from pi1: " K3S_VERSION
read -r -s -p "Join token from pi1: " K3S_TOKEN
echo
export INSTALL_K3S_VERSION="$K3S_VERSION"
export K3S_URL="https://192.168.1.101:6443"
export K3S_TOKEN

curl -fsSL https://get.k3s.io -o install-k3s.sh
less install-k3s.sh
sh install-k3s.sh agent \
  --docker \
  --node-name pi2 \
  --node-ip 192.168.1.102

unset K3S_TOKEN K3S_VERSION INSTALL_K3S_VERSION K3S_URL
```

The installer requests sudo privileges as needed. Using the same K3s version
prevents server/agent version drift.

Back on **pi1**:

```bash
sudo k3s kubectl wait --for=condition=Ready node/pi2 --timeout=180s
sudo k3s kubectl get nodes -o wide
sudo k3s kubectl get nodes \
  -o custom-columns=NAME:.metadata.name,RUNTIME:.status.nodeInfo.containerRuntimeVersion
```

Both nodes should be `Ready`, with runtime `docker://...`.

## 5. Deploy OpenSearch on pi2

Run this and all remaining Kubernetes commands on **pi1**. The Kubernetes CLI
is bundled with K3s; a separate `kubectl` installation is not needed.

The OpenSearch settings match `server/compose.pull.yaml`: single-node mode,
security disabled, 512 MB Java heap, and memory-mapped storage disabled. Its
2 GiB memory limit leaves room for native memory beyond the Java heap. Port 9200
is **cluster-internal only**, not a NodePort or host port.

```bash
mkdir -p ~/osdu-lite-rpi
cd ~/osdu-lite-rpi

cat > opensearch.yaml <<'YAML'
apiVersion: v1
kind: Namespace
metadata:
  name: osdu-lite
---
apiVersion: v1
kind: Service
metadata:
  name: opensearch
  namespace: osdu-lite
spec:
  selector:
    app: opensearch
  ports:
    - name: http
      port: 9200
      targetPort: 9200
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: opensearch
  namespace: osdu-lite
spec:
  replicas: 1
  strategy:
    type: Recreate
  selector:
    matchLabels:
      app: opensearch
  template:
    metadata:
      labels:
        app: opensearch
    spec:
      nodeSelector:
        kubernetes.io/hostname: pi2
      securityContext:
        fsGroup: 1000
      containers:
        - name: opensearch
          image: opensearchproject/opensearch:3.9.0
          ports:
            - containerPort: 9200
          env:
            - name: discovery.type
              value: single-node
            - name: DISABLE_INSTALL_DEMO_CONFIG
              value: "true"
            - name: DISABLE_SECURITY_PLUGIN
              value: "true"
            - name: OPENSEARCH_JAVA_OPTS
              value: "-Xms512m -Xmx512m"
            - name: node.store.allow_mmap
              value: "false"
          resources:
            requests:
              cpu: 500m
              memory: 1Gi
            limits:
              memory: 2Gi
          volumeMounts:
            - name: data
              mountPath: /usr/share/opensearch/data
          startupProbe:
            exec:
              command:
                - curl
                - -fsS
                - "http://localhost:9200/_cluster/health?wait_for_status=yellow&timeout=2s"
            periodSeconds: 5
            timeoutSeconds: 3
            failureThreshold: 120
          readinessProbe:
            exec:
              command:
                - curl
                - -fsS
                - "http://localhost:9200/_cluster/health?wait_for_status=yellow&timeout=2s"
            periodSeconds: 10
            timeoutSeconds: 3
      volumes:
        - name: data
          emptyDir: {}
YAML

sudo k3s kubectl apply -f opensearch.yaml
sudo k3s kubectl -n osdu-lite rollout status deployment/opensearch --timeout=600s
```

Wait for the rollout to succeed before continuing. A single OpenSearch node can
have **yellow** health because replica shards cannot be assigned; this is normal.
Disabling mmap matches the repository's development configuration and avoids
requiring a host-wide `vm.max_map_count` change.

## 6. Deploy the API on pi1

Still in `~/osdu-lite-rpi` on **pi1**:

```bash
cat > api.yaml <<'YAML'
apiVersion: apps/v1
kind: Deployment
metadata:
  name: osdu-lite
  namespace: osdu-lite
spec:
  replicas: 1
  strategy:
    type: Recreate
  selector:
    matchLabels:
      app: osdu-lite
  template:
    metadata:
      labels:
        app: osdu-lite
    spec:
      nodeSelector:
        kubernetes.io/hostname: pi1
      containers:
        - name: api
          image: ghcr.io/mehmetkadriumay/osdu-lite:latest
          imagePullPolicy: Always
          ports:
            - containerPort: 8089
          env:
            - name: PORT
              value: "8089"
            - name: OSDU_PARTITION
              value: company-prod
            - name: OSDU_TOKEN
              value: demo-token
            - name: REQUIRE_AUTH
              value: "true"
            - name: PRELOAD_FULL_SAMPLE
              value: "true"
            - name: INDEX_DELAY_SECONDS
              value: "0.7"
            - name: OPENSEARCH_URL
              value: http://opensearch:9200
          resources:
            requests:
              cpu: 100m
              memory: 128Mi
            limits:
              memory: 512Mi
          startupProbe:
            httpGet:
              path: /health
              port: 8089
            periodSeconds: 5
            timeoutSeconds: 3
            failureThreshold: 60
          readinessProbe:
            httpGet:
              path: /health
              port: 8089
            periodSeconds: 5
            timeoutSeconds: 3
---
apiVersion: v1
kind: Service
metadata:
  name: osdu-lite
  namespace: osdu-lite
spec:
  type: NodePort
  selector:
    app: osdu-lite
  ports:
    - name: http
      port: 8089
      targetPort: 8089
      nodePort: 30089
YAML

sudo k3s kubectl apply -f api.yaml
sudo k3s kubectl -n osdu-lite rollout status deployment/osdu-lite --timeout=600s
sudo k3s kubectl -n osdu-lite get pods -o wide
```

Expect one ready API pod on `pi1` and one ready OpenSearch pod on `pi2`. These
are the ARM64-capable image names used by the repository's published-image
quickstart; the API image is in the upstream `mehmetkadriumay` namespace, not
automatically in your fork's namespace. `latest` is convenient but mutable;
use an available release tag or image digest when you need repeatability.

There is deliberately no API liveness probe: `/health` includes search health,
so using it to force restarts during a search outage could destroy in-memory
API state. Readiness removes the API from service until it is healthy again.

## 7. Check the API and run the existing smoke checks

From **pi1**, or a trusted LAN machine with curl:

```bash
curl -fsS http://192.168.1.101:30089/health

curl -fsS http://192.168.1.101:30089/api/search/v2/query \
  -H 'Authorization: Bearer demo-token' \
  -H 'data-partition-id: company-prod' \
  -H 'Content-Type: application/json' \
  --data '{"kind":"osdu:wks:master-data--Well:*","query":"data.FacilityName:\"EAGLE-01\"","limit":10}'
```

Health should report `status: UP` and, on a fresh startup, `storedRecords: 15`.
The search should return the sample EAGLE-01 well. Your application base URL is
**`http://192.168.1.101:30089`**, not port 8089 on the host. Kubernetes NodePort
also normally serves the same API through `192.168.1.102:30089`.

To run the repository's existing checks, on **pi1**:

```bash
cd ~
git clone https://github.com/sohaibqasem/osdu-lite.git
cd osdu-lite
export OSDU_BASE_URL=http://192.168.1.101:30089
bash server/smoke-test.sh
bash server/reference-smoke-test.sh
python3 server/community-smoke-test.py
```

If you already cloned the repository, use that checkout instead. These checks
use Bash, curl, and Python's standard library; no Java, Maven, or local pyproj
installation is needed. Some checks exercise write/delete lifecycles, so run
them against this disposable demo, not an instance another person relies on.

**Security:** the known demo token is an all-groups token, and HTTP is
unencrypted. OpenSearch has no authentication. Keep the entire setup on a
trusted private LAN with no untrusted cluster tenants. This is not a hardened
or Internet-facing installation.

## 8. Day-to-day commands

On **pi1**:

```bash
# See placement, readiness, and recent application output.
sudo k3s kubectl -n osdu-lite get pods -o wide
sudo k3s kubectl -n osdu-lite logs deployment/osdu-lite --tail=80
sudo k3s kubectl -n osdu-lite logs deployment/opensearch --tail=80

# Reset API state and reload the bundled sample (discards your changes).
sudo k3s kubectl -n osdu-lite rollout restart deployment/osdu-lite
sudo k3s kubectl -n osdu-lite rollout status deployment/osdu-lite --timeout=600s
```

Docker and K3s start automatically after reboot. If both Pis reboot, OpenSearch
may take longer; the API can fail startup and be retried by Kubernetes until
search becomes reachable. All API process restarts reset its state.

To pull a newer API image deliberately, run the API restart commands above:
`imagePullPolicy: Always` checks the registry on container creation. This also
resets API state. For reproducible deployments, edit `api.yaml` to use your
chosen release tag/digest and reapply it instead.

To stop only this application, leaving the cluster installed:

```bash
sudo k3s kubectl -n osdu-lite scale deployment/osdu-lite --replicas=0
sudo k3s kubectl -n osdu-lite scale deployment/opensearch --replicas=0
```

To start it again:

```bash
sudo k3s kubectl -n osdu-lite scale deployment/opensearch --replicas=1
sudo k3s kubectl -n osdu-lite rollout status deployment/opensearch --timeout=600s
sudo k3s kubectl -n osdu-lite scale deployment/osdu-lite --replicas=1
sudo k3s kubectl -n osdu-lite rollout status deployment/osdu-lite --timeout=600s
```

To remove **only this demo and all its temporary data**:

```bash
sudo k3s kubectl delete namespace osdu-lite
```

## Troubleshooting

Run these on **pi1** first:

```bash
sudo k3s kubectl get nodes -o wide
sudo k3s kubectl -n osdu-lite get pods -o wide
sudo k3s kubectl -n osdu-lite get events --sort-by=.metadata.creationTimestamp
sudo k3s kubectl -n osdu-lite describe pods
```

| Symptom | Check or fix |
| --- | --- |
| Node does not join | Confirm the server address, private token, matching K3s versions, and TCP 6443 connectivity. On pi2, inspect `sudo journalctl -u k3s-agent -n 80 --no-pager`. |
| K3s server fails | On pi1, inspect `sudo journalctl -u k3s -n 80 --no-pager`; check swap and memory cgroups. |
| Pod stays `Pending` | Check node names are exactly `pi1` and `pi2`, both are Ready, and `describe pods` does not report insufficient resources. Placement is pinned intentionally, so a failed Pi is not automatically replaced by the other. |
| `ImagePullBackOff` | Check Internet/DNS access, image name, registry availability, and ARM64 support. Inspect pod events for the actual error. |
| API `CrashLoopBackOff` | Check `sudo k3s kubectl -n osdu-lite logs deployment/osdu-lite --previous`. Confirm OpenSearch is Ready and its Service exists. Cross-node failures usually require checking UDP 8472 and firewall forwarding rules. |
| OpenSearch `OOMKilled` | Check free memory and competing workloads on pi2. The 2 GiB limit includes heap and native memory; raise it only if necessary. |
| Search fails after OpenSearch pod replacement | Its `emptyDir` may have been lost. Restart the API to rebuild the sample index; this also discards custom API state. |
| API works on a Pi but not your workstation | Check the actual LAN address, TCP 30089, VLAN/router isolation, and that the API pod is Ready. Do not expose port 9200 as a workaround. |

If the memory cgroup is disabled, on the **affected Pi** edit
`/boot/firmware/cmdline.txt` with `sudo nano`. Append
`cgroup_memory=1 cgroup_enable=memory` to the **existing single line**, without
removing existing options or introducing a second line. Reboot and check
`grep -w memory /proc/cgroups` again.

If the published API image is unavailable, you can build the repository image
directly on **pi1**, where the API is pinned. From your repository checkout:

```bash
sudo docker build -t osdu-lite:rpi ./server
sudo k3s kubectl -n osdu-lite patch deployment osdu-lite --type=strategic \
  -p '{"spec":{"template":{"spec":{"containers":[{"name":"api","image":"osdu-lite:rpi","imagePullPolicy":"Never"}]}}}}'
sudo k3s kubectl -n osdu-lite rollout status deployment/osdu-lite --timeout=600s
```

This works because K3s is using that node's Docker engine. Update `api.yaml` to
the same image and pull policy if you intend to keep using the local build;
otherwise reapplying the original file restores the published-image settings.

## References

- [Repository runtime settings and limitations](README.md)
- [Docker Engine installation on Ubuntu](https://docs.docker.com/engine/install/ubuntu/)
- [K3s requirements and networking](https://docs.k3s.io/installation/requirements)
- [K3s with Docker as its container runtime](https://docs.k3s.io/advanced#using-docker-as-the-container-runtime)
