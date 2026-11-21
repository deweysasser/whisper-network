# Security policy

## Supported versions

whisper-network is pre-1.0. Security fixes are made on the `main` branch and
released in the next tagged version. Older tags are not patched; upgrade to the
latest release.

| Version | Supported |
|---|---|
| latest release and `main` | yes |
| anything older | no |

## Reporting a vulnerability

Use GitHub's private vulnerability reporting: open the repository's
**Security** tab and choose **Report a vulnerability**. Please do not open a
public issue or pull request for a suspected vulnerability.

Include the version or commit, how you deployed it, and steps to reproduce. You
should get an acknowledgement within a few days. Fixes are developed privately
and disclosed with the release that contains them.

## Threat model

whisper-network is operated by trusted administrators. Its job is to limit what
untrusted inputs and partially trusted machines can do.

| Threat | Mitigation |
|---|---|
| Malicious media exploits ffmpeg | ffmpeg runs only in worker `prepare` tasks, in a child under `sandbox-exec` (DESIGN.md §7.4). Landlock limits the filesystem to the task's files, the tool and system libraries, and (on kernels that have the rights) TCP, UDP, pathname Unix sockets, abstract Unix sockets and signals. A seccomp filter refuses `socket` and `socketpair` for every family, `io_uring`, new namespaces (`unshare`/`clone` with `CLONE_NEW*`, `setns`; `clone3` reports `ENOSYS`), leaving the process group (`setsid`/`setpgid`), and, below Landlock ABI 6, signals outside the sandbox. `no_new_privs`, an empty capability set, an allow-listed environment, rlimits (address space, file size, open files, `RLIMIT_NPROC`) and a timeout apply; the worker kills the child's whole process group when it ends or is cancelled. A root worker must name a dedicated, unprivileged `--child-uid`. ffmpeg runs with `-loglevel error`, so the input's metadata stays out of logs. Prepare tasks go only to workers whose self-test reaches `--prepare-min-sandbox`. |
| Malicious media exploits whisper-cli | whisper-cli only receives server-generated PCM WAV, and runs under the same sandbox. |
| Compromised child attacks the worker | The worker creates every file it hands a child with `O_CREAT|O_EXCL|O_NOFOLLOW`, mode 0600; it reads outputs with `O_NOFOLLOW|O_NONBLOCK`, requires a single-link regular file and bounds its size. Input downloads stop at the size the lease states, and a lease without a size is refused. The worker's HTTP client refuses redirects to another origin. |
| Malicious audio from a compromised worker | The server validates it by a full pure-Go decode with size and duration bounds. |
| Rogue worker registers to read media | Registration grants nothing. A human approves each worker, per queue, and every approval is audited. There are no enrollment tokens. |
| Stale or partitioned worker overwrites a result | Lease IDs fence every worker call; non-live leases get `410 Gone`. |
| Registration spam | Per-IP rate limit, a cap on pending workers, and a TTL that deletes stale ones. |
| Job ID enumeration | Authorization decides access; jobs a caller cannot see return 404. |
| Credential theft from the database | Tokens are stored as sha256 hashes (they are 256-bit random); passwords use argon2id. |
| Default credentials | The first-run admin password is random, shown once, and must be changed at first login. |
| Secrets in logs | Tokens, passwords, cookies, prompts, transcript text and job metadata are never logged; a test enforces it. A URL job's URL is not logged, audited or used as a metric label (its site stands for it), except that a failed fetch's reason and stderr tail are kept in full and may quote it. |
| URL jobs reach internal services (SSRF) | The server never contacts a submitted URL. Downloaders fetch through an in-process egress proxy that refuses private, loopback, link-local, multicast and IPv4-embedding IPv6 ranges after DNS resolution, on every redirect hop, and allows only ports 80 and 443 (DESIGN.md §7.6, §13). Submitting needs the separate `submit_url` grant, and a downloader holds the separate `fetch` grant. |
| Malicious site exploits yt-dlp | yt-dlp runs under `sandbox-exec` with no filesystem access beyond its work directory and tools, and may open TCP connections only to the proxy's port (Landlock ABI 4); seccomp refuses other socket families, `listen`, `accept` and TCP Fast Open. A downloader never runs ffmpeg or whisper-cli, and the server checks the fetched result before any worker processes it. |
| Privilege escalation through client creation | A client can only hand out grants it holds. |
| Disk exhaustion | Per-queue upload caps, a free-space check on `/readyz` and on upload. |
| Model file tampering | Each model is pinned by sha256, verified by the server before caching and by the worker before use. |

### Known residual risks

- The sandbox needs Linux on amd64 or arm64. Without Landlock the worker
  reports sandbox level `none` (always the case on macOS and Windows); where
  the seccomp filter is unavailable, sockets stay open and the self-test
  reports `fs-only`. The level is measured at startup, not assumed from the
  kernel version.
- All children of one worker share a uid (the worker's own, or
  `--child-uid`), and with it per-uid kernel limits such as `RLIMIT_NPROC`.
  One child can use up the process allowance of the others. Each child runs
  in its own Landlock domain, which stops `ptrace` between them.
- The `--child-uid` must be dedicated to whisper-network. The worker refuses
  to start while anything else runs as that uid, but processes in other PID
  namespaces (another container with the same uid) are invisible to it, still
  count against `RLIMIT_NPROC`, and share whatever files that uid can reach.
- The server is a single process with a single SQLite file. A compromise of the
  server host exposes all queues.
- Queue administrators are trusted operators; there is no hostile multi-tenant
  isolation between them.
- Plain HTTP exposes tokens. The server warns when it serves HTTP on a
  non-loopback address.
- Landlock limits yt-dlp by port, not address: a compromised yt-dlp could
  connect directly to an attacker's host on the proxy's port, bypassing the
  egress filter. An egress firewall on the downloader (only TCP 80, 443 and
  the server's port, plus DNS) closes this; see README and DESIGN.md §13.
- A `--yt-dlp-cookies` file is readable by yt-dlp inside the sandbox, and
  anyone with `submit_url` fetches with that account's session. Use a
  throwaway account.

## Hardening checklist for operators

- [ ] **TLS.** Terminate TLS in a reverse proxy or pass `--tls-cert` and
      `--tls-key`. Consider `--require-tls`. Set `--trust-proxy` only when a
      proxy you control sets `X-Forwarded-For`, otherwise rate limits can be
      bypassed or spoofed. Client and worker tokens must not cross untrusted
      networks in cleartext.
- [ ] **Metrics listener private.** `--metrics-listen` (default `127.0.0.1:9090`,
      loopback only) is unauthenticated. If you widen it, bind a private interface, do not publish it, or set
      it empty to disable it.
- [ ] **Approve workers deliberately.** Check the description, hostname and
      sandbox level before approving, and approve only for the queues that
      worker should hear. Reject workers you do not recognise.
- [ ] **Downloaders.** Grant `submit_url` only to clients you trust to make
      your network fetch URLs. Run downloaders on their own machine or
      container behind an egress firewall (above), keep
      `--fetch-min-sandbox=full` (default), and leave `--allow-private-egress`
      off outside tests.
- [ ] **Prepare sandbox policy.** Keep `--prepare-min-sandbox=full` (default)
      unless you trust every worker; lowering it sends untrusted media to
      workers with weaker sandboxes. Workers that should not run ffmpeg can set
      `--kinds transcribe`.
- [ ] **A Landlock kernel on workers, amd64 or arm64.** Landlock (5.13+) is
      what confines the filesystem; newer kernels add Landlock network rights
      and (6.12+) signal and abstract-socket scoping as a second layer behind
      the seccomp filter. Check the reported sandbox level in
      `admin list-workers` or the UI.
- [ ] **Do not run workers as root.** The worker refuses to run as root unless
      `--child-uid` and `--child-gid` are set, so the child processes still run
      unprivileged. That uid must be dedicated to the worker's children: not
      `nobody`, not a login user, not a uid any other service or container
      runs as. The official worker image runs as a non-root user (uid 10001);
      keep it that way. The server image is distroless and non-root, and its `/data`
      directory is mode 0700 owned by uid 65532.
- [ ] **Mount volumes.** Persist the server `--data` directory (and back it up
      with `sqlite3 .backup` or `VACUUM INTO` plus `blobs/`). Persist the worker
      `--state` directory, otherwise each restart registers a new pending worker.
- [ ] **Give workers resource limits.** Set a low CPU share and a memory limit on
      worker containers; the worker cannot limit itself.
- [ ] **Change the admin password** at first login, and use per-purpose client
      tokens with the least grants needed (`submit` on one queue).
- [ ] **Do not ship the first-run password banner** to a log store.
- [ ] **Pin image tags** (or digests) instead of `latest`, and update regularly.
      Branch builds are published only as `br-<branch>` and never as `latest`,
      `stable` or `v*`.
- [ ] **Restrict who can reach the API** (firewall, VPN, or proxy authentication)
      where the clients are a known set.
