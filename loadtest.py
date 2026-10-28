import argparse
import math
import multiprocessing as mp
import os
import queue
import random
import signal
import statistics
import time
import uuid
from collections import Counter, defaultdict

import boto3

from credential_pool import CredentialPool, S3LeaseBackend

CRASH_EXIT_CODE = 3


def parse_args():
    parser = argparse.ArgumentParser(description="Hammer CredentialPool on S3 from many processes and check for overlaps.")
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--prefix", default=f"credential-pool-loadtest/{uuid.uuid4().hex[:8]}/")
    parser.add_argument("--profile")
    parser.add_argument("--region")
    parser.add_argument("--endpoint-url")
    parser.add_argument("--credentials", type=int, default=10)
    parser.add_argument("--workers", type=int, default=30)
    parser.add_argument("--duration", type=float, default=120, help="seconds to keep claiming")
    parser.add_argument("--ttl", type=float, default=10)
    parser.add_argument("--margin", type=float, default=2)
    parser.add_argument("--hold-min", type=float, default=0.5)
    parser.add_argument("--hold-max", type=float, default=20)
    parser.add_argument("--claim-timeout", type=float, default=30)
    parser.add_argument("--crash-rate", type=float, default=0.05, help="fraction of leases abandoned by a hard exit")
    parser.add_argument("--freeze-every", type=float, default=10, help="seconds between SIGSTOPs of a random lease holder, 0 disables")
    parser.add_argument("--freeze-for", type=float, help="seconds a frozen worker stays stopped, default 1.5 x ttl")
    return parser.parse_args()


def work(args, results, holding, stop_at):
    s3 = boto3.Session(profile_name=args.profile, region_name=args.region).client("s3", endpoint_url=args.endpoint_url)
    pool = CredentialPool(S3LeaseBackend(s3, args.bucket, args.prefix), ttl=args.ttl, margin=args.margin)
    ids = [f"cred-{i}" for i in range(args.credentials)]
    while time.time() < stop_at:
        asked = time.monotonic()
        lease = pool.claim(ids, timeout=args.claim_timeout)
        waited = time.monotonic() - asked
        if lease is None:
            results.put(("timeout", None, waited, None, None))
            continue
        holding.value = True
        hold_until = time.monotonic() + random.uniform(args.hold_min, args.hold_max)
        first = last = None
        while (now := time.monotonic()) < hold_until and lease.held:
            first = first or now
            last = now
            time.sleep(0.05)
        if random.random() < args.crash_rate:
            results.put(("crashed", lease.id, waited, first, last))
            results.close()
            results.join_thread()
            os._exit(CRASH_EXIT_CODE)
        results.put(("released" if lease.held else "lost", lease.id, waited, first, last))
        holding.value = False
        pool.release(lease)


def drain(results, records):
    while True:
        try:
            records.append(results.get_nowait())
        except queue.Empty:
            return


def run(args):
    ctx = mp.get_context("spawn")
    results = ctx.Queue()
    stop_at = time.time() + args.duration
    freeze_for = args.freeze_for or 1.5 * args.ttl

    def start():
        holding = ctx.Value("b", False, lock=False)
        process = ctx.Process(target=work, args=(args, results, holding, stop_at), daemon=True)
        process.start()
        return process, holding

    workers = [start() for _ in range(args.workers)]
    records, exited, frozen, freezes = [], [], {}, 0
    next_freeze = time.monotonic() + args.freeze_every if args.freeze_every else math.inf
    try:
        while time.time() < stop_at or any(w.is_alive() for w, _ in workers):
            drain(results, records)
            now = time.monotonic()
            running = time.time() < stop_at
            if running:
                for i, (worker, _) in enumerate(workers):
                    if not worker.is_alive():
                        exited.append(worker)
                        workers[i] = start()
            for pid, resume_at in list(frozen.items()):
                if now >= resume_at:
                    os.kill(pid, signal.SIGCONT)
                    del frozen[pid]
            candidates = [w for w, h in workers if h.value and w.is_alive() and w.pid not in frozen]
            if running and now >= next_freeze and candidates:
                victim = random.choice(candidates)
                os.kill(victim.pid, signal.SIGSTOP)
                frozen[victim.pid] = now + freeze_for
                freezes += 1
                next_freeze = now + args.freeze_every
            time.sleep(0.1)
        drain(results, records)
    finally:
        for pid in frozen:
            os.kill(pid, signal.SIGCONT)
    errors = sum(w.exitcode not in (0, CRASH_EXIT_CODE) for w in exited + [w for w, _ in workers])
    return records, freezes, errors


def report(args, records, freezes, errors):
    outcomes = Counter(record[0] for record in records)
    waits = [record[2] for record in records if record[0] != "timeout"]
    spans = defaultdict(list)
    for _, credential_id, _, first, last in records:
        if first is not None:
            spans[credential_id].append((first, last))

    overlaps = []
    for credential_id, credential_spans in spans.items():
        credential_spans.sort()
        busy_until = -math.inf
        for first, last in credential_spans:
            if first <= busy_until:
                overlaps.append((credential_id, busy_until - first))
            busy_until = max(busy_until, last)

    all_spans = [span for credential_spans in spans.values() for span in credential_spans]
    window = max(last for _, last in all_spans) - min(first for first, _ in all_spans)
    used = sum(last - first for first, last in all_spans)
    p50, p95, p99 = (statistics.quantiles(waits, n=100, method="inclusive")[i] for i in (49, 94, 98))

    print(f"prefix        {args.prefix}")
    print(f"setup         {args.workers} workers, {args.credentials} credentials, ttl {args.ttl}s, margin {args.margin}s")
    print(f"claims        {len(waits)} ok, {outcomes['timeout']} timed out")
    print(f"claim wait    p50 {p50:.2f}s  p95 {p95:.2f}s  p99 {p99:.2f}s  max {max(waits):.2f}s")
    print(f"outcomes      {outcomes['released']} released, {outcomes['lost']} lost, {outcomes['crashed']} crashed, {freezes} freezes")
    print(f"utilization   {used / (window * args.credentials):.1%} of credential time in use")
    print(f"worker errors {errors}")
    print(f"overlaps      {len(overlaps)}")
    for credential_id, seconds in overlaps:
        print(f"  {credential_id} used by two holders for {seconds:.3f}s")
    return not overlaps and not errors


if __name__ == "__main__":
    arguments = parse_args()
    raise SystemExit(0 if report(arguments, *run(arguments)) else 1)
