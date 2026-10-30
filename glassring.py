#!/usr/bin/env python3
"""
glassring: deterministic chaos lab for a Dynamo-style replicated store.
One file, zero dependencies.

  Ring            consistent hashing, virtual nodes, N-way preference list
  VClock          per-key vector clock: dominates / concurrent / merge
  Node            local store: concurrent writes kept as siblings, not
                   silently overwritten; stale siblings pruned on merge
  Coordinator     sloppy quorum PUT/GET (N=3, W=2, R=2), read-repair,
                   hinted handoff when a preferred replica is unreachable
  Sim             tick-based chaos: partitions, crashes, data-loss restarts,
                   concurrent clients that reconcile siblings like a real
                   Dynamo application is required to
  checkers        no-lost-write-under-quorum, sibling convergence, hinted-
                   handoff delivery -- all against an independent ledger
  shrink          delta-debugging: failing run -> minimal fault schedule
  MUTANTS         four real Dynamo-style implementation bugs, injected on
                   purpose

Usage:
  python glassring.py selftest
  python glassring.py hunt [seeds]
  python glassring.py shrink <mutant> [seed]
  python glassring.py run <seed> [mutant]
  python glassring.py bench

Research and education tool. Simulated nodes only, no real network.
"""
from __future__ import annotations
import bisect, hashlib, sys, time

MUTANTS = {
    "clobber_siblings": "put() overwrites blindly instead of keeping concurrent siblings",
    "no_read_repair":   "get() never pushes the merged version back to stale replicas",
    "hint_drop":        "a stashed hint is discarded instead of replayed once its target revives",
    "quorum_off_by_one": "the coordinator accepts W=1 ack as a full quorum regardless of configured W",
}
KEYS = ["user:1", "user:2", "cart:7", "counter:x"]
N, W, R = 3, 2, 2


class Rng:
    M = (1 << 64) - 1

    def __init__(s, seed): s.x = seed & s.M

    def next(s):
        s.x = (s.x + 0x9E3779B97F4A7C15) & s.M
        z = s.x
        z = ((z ^ (z >> 30)) * 0xBF58476D1CE4E5B9) & s.M
        z = ((z ^ (z >> 27)) * 0x94D049BB133111EB) & s.M
        return z ^ (z >> 31)

    def below(s, n): return s.next() % n
    def between(s, a, b): return a + s.next() % (b - a + 1)


def h64(x: str) -> int:
    return int.from_bytes(hashlib.blake2b(x.encode(), digest_size=8).digest(), "big")


# ------------------------------------------------------------------ ring
class Ring:
    def __init__(s, n_nodes, vnodes=64):
        s.vnodes, s._ring = vnodes, []
        for i in range(n_nodes):
            for v in range(vnodes):
                bisect.insort(s._ring, (h64(f"n{i}#{v}"), i))

    def preference_list(s, key: str, n: int) -> list[int]:
        start = bisect.bisect(s._ring, (h64(key), -1)) % len(s._ring)
        out = []
        for i in range(len(s._ring)):
            node = s._ring[(start + i) % len(s._ring)][1]
            if node not in out:
                out.append(node)
            if len(out) == n:
                break
        return out


# ------------------------------------------------------------------ vector clocks
def vdom(a: dict, b: dict) -> bool:
    """True if a dominates (happened-after) b: a >= b in every slot, a != b."""
    if a == b: return False
    return all(a.get(k, 0) >= v for k, v in b.items()) and all(v >= b.get(k, 0) for k, v in a.items())


def vmerge(a: dict, b: dict) -> dict:
    out = dict(a)
    for k, v in b.items(): out[k] = max(out.get(k, 0), v)
    return out


# ------------------------------------------------------------------ node
class Node:
    """nid is the stable physical slot (routing, ring membership). actor is the
    vclock identity: (nid, incarnation), plus a clock that always increments on
    every write this node coordinates, regardless of what context the caller
    supplied. A data-loss restart mints a fresh incarnation and resets the
    clock to zero -- reusing the old identity after state is gone would let a
    stale or empty write context collide with a counter value the node had
    already handed out before the crash, silently clobbering an unrelated
    write that compares as vclock-equal (the same actor-identity lesson as
    stillpoint's CRDT replicas, independently rediscovered here)."""

    def __init__(s, nid, mutant):
        s.nid, s.mutant, s.alive, s.incarnation = nid, mutant, True, 0
        s.reset()

    def reset(s):
        s.actor = (s.nid, s.incarnation)
        s.clock = 0
        s.store: dict[str, list[tuple[dict, str]]] = {}   # key -> [(vclock, value), ...] siblings
        s.hints: dict[int, list] = {}                       # target_nid -> [(key, vclock, value), ...]

    def crash(s): s.alive = False   # NOTE: see Sim.fault -- hints held here are abandoned on crash

    def revive(s): s.alive = True                      # process restart: disk survives, data intact

    def restart(s): s.alive = True; s.incarnation += 1; s.reset()   # true data loss: fresh actor identity

    def next_clock(s):
        s.clock += 1
        return s.clock

    def put(s, key, vclock, value):
        existing = s.store.get(key, [])
        if s.mutant == "clobber_siblings":
            s.store[key] = [(vclock, value)]
            return
        kept = [(vc, v) for vc, v in existing if not vdom(vclock, vc)]
        if any(vc == vclock or vdom(vc, vclock) for vc, v in kept):
            return                                            # stale write, already superseded
        kept.append((vclock, value))
        s.store[key] = kept

    def get(s, key):
        return list(s.store.get(key, []))

    def stash_hint(s, target, key, vclock, value):
        s.hints.setdefault(target, []).append((key, vclock, value))


# ------------------------------------------------------------------ coordinator
class Coordinator:
    def __init__(s, sim):
        s.sim = sim

    def reachable(s, frm, to):
        sim = s.sim
        if not sim.nodes[to].alive: return False
        if sim.group[frm] != sim.group[to]: return False
        return sim.rng.below(1000) >= sim.drop_pm

    def do_put(s, client_id, key, value, ctx: dict):
        sim = s.sim
        pref = sim.ring.preference_list(key, N)
        coordinator = next((p for p in pref if sim.nodes[p].alive), pref[0])
        actor = sim.nodes[coordinator].actor
        vclock = dict(ctx); vclock[actor] = sim.nodes[coordinator].next_clock()
        strict_acks, acked, used_hints = 0, 0, []
        for target in pref:
            if s.reachable(coordinator, target):
                sim.nodes[target].put(key, vclock, value)
                acked += 1; strict_acks += 1
            else:
                fallback = next((p for p in pref if p != target and sim.nodes[p].alive
                                  and s.reachable(coordinator, p)), None)
                if fallback is None:
                    fallback = next((n.nid for n in sim.nodes if n.alive
                                      and s.reachable(coordinator, n.nid) and n.nid not in pref), None)
                if fallback is not None:                       # hint_drop only breaks replay, not creation
                    sim.nodes[fallback].stash_hint(target, key, vclock, value)
                    tag = (target, key, tuple(sorted(vclock.items())))
                    sim.hints_created.append((target, key, vclock))
                    used_hints.append(tag)
                    acked += 1                                  # sloppy quorum: a durable hint counts
        need = 1 if sim.mutant == "quorum_off_by_one" else W
        if acked >= need and acked < W:
            sim.violate("quorum-under-replicated-ack", f"key {key!r} was acknowledged as committed "
                        f"with only {acked} of the required W={W} replicas/hints reached")
        if acked >= need:
            # strict_acks counts replicas that hold the data directly, right now; the remainder
            # of the W quorum was satisfied by hints that still need to be delivered. The R+W>N
            # guarantee is unconditional once >=W replicas hold the data directly. When it was
            # only reached via hints, the guarantee holds once those hints land -- and, same as
            # real sloppy-quorum systems, is not guaranteed if a hint is lost before that (see
            # Sim._check_quorum_history and the README).
            sim.ledger.append((key, vclock, value, sim.t, strict_acks, tuple(used_hints)))
            return vclock
        return None

    def anti_entropy(s, key):
        """Full repair across every alive, reachable preference-list member -- not limited to
        R. Real systems run this continuously in the background (Merkle-tree sync); without it,
        a replica that restarts empty and is never queried again stays empty forever, and
        sequential data-loss across the whole preference list (never simultaneous, always
        within the N-W=1 tolerance at any instant) can still erase a key over time. This is
        what actually earns the durability guarantee under sustained chaos."""
        sim = s.sim
        pref = sim.ring.preference_list(key, N)
        live = [p for p in pref if sim.nodes[p].alive]
        if len(live) < 2: return
        reachable_pairs = [(a, b) for a in live for b in live if a != b and s.reachable(a, b)]
        if not reachable_pairs: return
        siblings: list[tuple[dict, str]] = []
        touched = set()
        for p in live:
            for vc, v in sim.nodes[p].get(key):
                siblings = [(ovc, ov) for ovc, ov in siblings if not vdom(vc, ovc)]
                if not any(ovc == vc or vdom(ovc, vc) for ovc, ov in siblings):
                    siblings.append((vc, v))
            touched.add(p)
        if sim.mutant == "no_read_repair":
            return
        for p in live:
            if sim.nodes[p].get(key) != siblings:
                for vc, v in siblings:
                    sim.nodes[p].put(key, vc, v)

    def do_get(s, client_id, key):
        sim = s.sim
        pref = sim.ring.preference_list(key, N)
        coordinator = next((p for p in pref if sim.nodes[p].alive), pref[0])
        responses = []
        for target in pref:
            if s.reachable(coordinator, target):
                responses.append((target, sim.nodes[target].get(key)))
        if len(responses) < R:
            return None
        siblings: list[tuple[dict, str]] = []
        for _, resp in responses:
            for vc, v in resp:
                siblings = [(ovc, ov) for ovc, ov in siblings if not vdom(vc, ovc)]
                if not any(ovc == vc or vdom(ovc, vc) for ovc, ov in siblings):
                    siblings.append((vc, v))
        if sim.mutant != "no_read_repair":
            for target, resp in responses:
                if resp != siblings:
                    for vc, v in siblings:
                        sim.nodes[target].put(key, vc, v)
        return siblings


# ------------------------------------------------------------------ nemesis
def gen_schedule(seed, n_nodes, horizon):
    r, out, t = Rng(seed * 0x9E3779B1 + 1313), [], 60
    while t < horizon - 30:
        x = r.below(100)
        if x < 30:
            pick = set()
            while len(pick) < 1 + r.below(max(1, n_nodes // 2)): pick.add(r.below(n_nodes))
            out.append((t, "part", tuple(sorted(pick))))
        elif x < 52: out.append((t, "heal", None))
        elif x < 72: out.append((t, "crash", r.below(n_nodes)))
        elif x < 86: out.append((t, "revive", r.below(n_nodes)))     # clean restart: disk intact
        elif x < 92: out.append((t, "wipe", r.below(n_nodes)))       # rare: true data loss
        else: out.append((t, "bounce", r.below(n_nodes)))
        t += r.between(45, 140)        # more spacing: let the cluster re-replicate between crashes
    return out


# ------------------------------------------------------------------ simulator
class Sim:
    def __init__(s, seed, mutant=None, horizon=2400, n_nodes=5, schedule=None):
        s.seed, s.mutant, s.horizon, s.n_nodes = seed, mutant, horizon, n_nodes
        s.rng, s.t, s.drop_pm = Rng(seed), 0, 6
        s.ring = Ring(n_nodes)
        s.nodes = [Node(i, mutant) for i in range(n_nodes)]
        s.group = [0] * n_nodes
        s.coord = Coordinator(s)
        s.ledger: list[tuple] = []                 # (key, vclock, value, tick) of every acked PUT
        s.hints_created: list[tuple] = []           # (target, key, vclock)
        s.hints_delivered: set = set()
        s.abandoned: set = set()
        s.viol: list[tuple] = []
        s.schedule = gen_schedule(seed, n_nodes, horizon) if schedule is None else list(schedule)
        s.wcounter = 0

    def violate(s, kind, detail):
        s.viol.append((kind, detail))

    def down(s): return sum(1 for n in s.nodes if not n.alive)

    def fault(s, a, arg):
        if a == "part": s.group = [1 if i in arg else 0 for i in range(s.n_nodes)]
        elif a == "heal": s.group = [0] * s.n_nodes
        elif a == "crash":
            if s.nodes[arg].alive and s.down() < 2:
                s._abandon_hints(s.nodes[arg]); s.nodes[arg].crash()
        elif a == "revive":
            if not s.nodes[arg].alive:
                nid = arg; s.nodes[nid].revive(); s._replay_hints(nid)
        elif a == "wipe":
            if not s.nodes[arg].alive:
                nid = arg; s.nodes[nid].restart(); s._replay_hints(nid)   # true data loss, fresh incarnation
        elif a == "bounce":
            if s.nodes[arg].alive and s.down() < 2:
                s._abandon_hints(s.nodes[arg]); s.nodes[arg].crash()
                s._pending = getattr(s, "_pending", []); s._pending.append(arg)

    def _abandon_hints(s, node):
        for target, stash in node.hints.items():
            for key, vclock, value in stash:
                s.abandoned.add((target, key, tuple(sorted(vclock.items()))))

    def _replay_hints(s, revived_nid):
        if s.mutant == "hint_drop":
            return
        for n in s.nodes:
            stash = n.hints.pop(revived_nid, [])
            for key, vclock, value in stash:
                if n.alive and s.coord.reachable(n.nid, revived_nid):
                    s.nodes[revived_nid].put(key, vclock, value)
                    s.hints_delivered.add((revived_nid, key, tuple(sorted(vclock.items()))))
                else:
                    n.hints.setdefault(revived_nid, []).append((key, vclock, value))

    def _tick_bounces(s):
        pend = getattr(s, "_pending", [])
        if pend and s.rng.below(100) < 35:
            nid = pend.pop(0)
            s.nodes[nid].revive(); s._replay_hints(nid)   # bounce: brief blip, disk intact

    def _check_quorum_history(s):
        for key, vclock, value, tick, strict, htags in s.ledger:
            if strict < W and any(h in s.abandoned for h in htags):
                continue    # sloppy-quorum weakness: see README "Strict vs sloppy durability"
            resp = s.coord.do_get("checker", key)
            if resp is None: continue
            joined = {}
            for vc, v in resp: joined = vmerge(joined, vc)
            if not (joined == vclock or vdom(joined, vclock)):
                s.violate("quorum-lost-write", f"key {key!r} commit {vclock} acked at t={tick} "
                          f"(strict_acks={strict}) is not visible to a quorum read (quorum join = {joined})")
                return

    def _check_convergence(s):
        for key in KEYS:
            pref = s.ring.preference_list(key, N)
            live = [s.nodes[p] for p in pref if s.nodes[p].alive]
            if len(live) < 2: continue
            ref = set((tuple(sorted(vc.items())), v) for vc, v in live[0].get(key))
            for n in live[1:]:
                got = set((tuple(sorted(vc.items())), v) for vc, v in n.get(key))
                if got != ref:
                    s.violate("sibling-divergence", f"key {key!r}: node {n.nid} and node {live[0].nid} "
                              f"disagree after quiescence")
                    return

    def _check_hints(s):
        for target, key, vclock in s.hints_created:
            tag = (target, key, tuple(sorted(vclock.items())))
            if s.nodes[target].alive and tag not in s.hints_delivered and tag not in s.abandoned:
                s.violate("hint-not-delivered", f"hint for key {key!r} -> node {target} was never replayed "
                          f"though the target is alive")
                return

    def run(s):
        crash_set = {t: a for t, a, arg in s.schedule for a2 in [a] if False}
        sched = {}
        for t, a, arg in s.schedule: sched.setdefault(t, []).append((a, arg))
        for tick in range(s.horizon):
            s.t = tick
            for a, arg in sched.get(tick, []): s.fault(a, arg)
            s._tick_bounces()
            x = s.rng.below(100)
            if x < 22:
                s.coord.anti_entropy(KEYS[s.rng.below(len(KEYS))])
            elif x < 70:
                key = KEYS[s.rng.below(len(KEYS))]
                cur = s.coord.do_get("c", key)
                if cur and len(cur) > 1:
                    value = "|".join(sorted(v for _, v in cur))
                    ctx = {}
                    for vc, _ in cur: ctx = vmerge(ctx, vc)
                else:
                    value = f"v{s.wcounter}"; s.wcounter += 1
                    ctx = cur[0][0] if cur else {}
                s.coord.do_put("c", key, value, ctx)
            else:
                s.coord.do_get("c", KEYS[s.rng.below(len(KEYS))])
        s.group, s.drop_pm = [0] * s.n_nodes, 0
        for n in s.nodes:
            if not n.alive: n.revive()
        for nid in range(s.n_nodes): s._replay_hints(nid)
        for _ in range(3):
            for key in KEYS: s.coord.anti_entropy(key)        # settle: full repair, not just R-of-N
        s._check_quorum_history()
        s._check_convergence()
        s._check_hints()
        kinds = sorted({v[0] for v in s.viol})
        return dict(bad=bool(kinds), kinds=kinds, detail=[v[1] for v in s.viol],
                    commits=len(s.ledger), hints=len(s.hints_created))


# ------------------------------------------------------------------ shrinker
def shrink(seed, mutant, horizon=2400, n_nodes=5, budget=250):
    sched, runs = list(gen_schedule(seed, n_nodes, horizon)), [0]

    def bad(sc, hz, nn):
        runs[0] += 1
        return Sim(seed, mutant, hz, nn, sc).run()["bad"]

    assert bad(sched, horizon, n_nodes), "seed does not fail"
    k = 2
    while len(sched) >= 2 and runs[0] < budget:
        chunk, hit = max(1, len(sched) // k), False
        for i in range(0, len(sched), chunk):
            cand = sched[:i] + sched[i + chunk:]
            if bad(cand, horizon, n_nodes): sched, k, hit = cand, max(k - 1, 2), True; break
        if not hit:
            if chunk == 1: break
            k = min(len(sched), k * 2)
    if bad([], horizon, n_nodes): sched = []
    while int(horizon * 0.8) >= 200:
        hz = int(horizon * 0.8); cand = [e for e in sched if e[0] < hz]
        if bad(cand, hz, n_nodes): horizon, sched = hz, cand
        else: break
    r = Sim(seed, mutant, horizon, n_nodes, sched).run()
    return dict(schedule=sched, horizon=horizon, n_nodes=n_nodes, runs=runs[0], result=r)


# ------------------------------------------------------------------ CLI
def sweep(mutant, seeds):
    fails, first, kinds = 0, None, {}
    for sd in range(1, seeds + 1):
        r = Sim(sd, mutant).run()
        if r["bad"]:
            fails += 1; first = first or sd
            for k in r["kinds"]: kinds[k] = kinds.get(k, 0) + 1
    return fails, first, kinds


def hunt(seeds=300):
    print(f"{seeds} seeds, N={N} W={W} R={R}, 5 nodes, {len(gen_schedule(1,5,2400))} faults/run\n")
    print(f"{'variant':<22}{'violations':>12}{'first seed':>12}   caught by")
    for m in (None,) + tuple(MUTANTS):
        f, first, kinds = sweep(m, seeds)
        print(f"{(m or 'correct ring'):<22}{f'{f}/{seeds}':>12}{(first or '-'):>12}   {', '.join(sorted(kinds)) or '-'}")


def show(sh, mutant, seed):
    r = sh["result"]
    print(f"{mutant} seed {seed}: minimal repro = {len(sh['schedule'])} fault(s), {sh['n_nodes']} nodes, "
          f"{sh['horizon']} ticks  ({sh['runs']} runs)")
    for t, a, arg in sh["schedule"]: print(f"  t={t:>5}  {a}{'' if arg is None else ' ' + str(arg)}")
    print("  violation:", ", ".join(r["kinds"]), "|", "; ".join(r["detail"]))


def bench():
    t0 = time.perf_counter()
    r = Sim(1, None, horizon=20000, n_nodes=9).run()
    dt = time.perf_counter() - t0
    print(f"9 nodes, 20,000 ticks: {dt:.2f}s wall ({20000/dt:,.0f} ticks/s), "
          f"{r['commits']} commits, {r['hints']} hints, bad={r['bad']}")


def selftest():
    a, b = {0: 1, 1: 1}, {0: 1}
    assert vdom(a, b) and not vdom(b, a) and not vdom(a, a)
    c = {0: 1, 2: 1}
    assert not vdom(a, c) and not vdom(c, a)        # genuinely concurrent
    assert vmerge(a, c) == {0: 1, 1: 1, 2: 1}
    print("vclock      OK  dominance and merge behave as expected on hand-built cases")

    a1, a2 = Sim(5).run(), Sim(5).run()
    assert a1 == a2
    print(f"determinism OK  seed 5 -> {a1['commits']} commits, {a1['hints']} hints, bit-identical on rerun")

    clean = [Sim(sd).run() for sd in range(1, 61)]
    bad = [r for r in clean if r["bad"]]
    assert not bad, [r["kinds"] for r in bad]
    print(f"ring layer  OK  60 chaotic seeds (partitions, crashes, data-loss restarts), 0 violations across "
          f"{sum(r['commits'] for r in clean)} commits and {sum(r['hints'] for r in clean)} hinted writes")

    first = {}
    for m in MUTANTS:
        for sd in range(1, 301):
            if Sim(sd, m).run()["bad"]: first[m] = sd; break
        assert m in first, f"mutant {m} escaped 300 seeds"
        print(f"mutant      OK  {m:<20} caught at seed {first[m]}")

    sh = shrink(first["clobber_siblings"], "clobber_siblings")
    full = len(gen_schedule(first["clobber_siblings"], 5, 2400))
    assert len(sh["schedule"]) <= full and sh["result"]["bad"]
    print(f"shrinker    OK  clobber_siblings: {full} faults -> {len(sh['schedule'])}, "
          f"5 nodes -> {sh['n_nodes']}, still fails")
    print("selftest: all OK")


if __name__ == "__main__":
    a = sys.argv[1:]
    if not a: print(__doc__)
    elif a[0] == "selftest": selftest()
    elif a[0] == "hunt": hunt(int(a[1]) if len(a) > 1 else 300)
    elif a[0] == "bench": bench()
    elif a[0] == "shrink":
        m = a[1] if len(a) > 1 else "clobber_siblings"
        sd = int(a[2]) if len(a) > 2 else next(x for x in range(1, 500) if Sim(x, m).run()["bad"])
        show(shrink(sd, m), m, sd)
    elif a[0] == "run":
        r = Sim(int(a[1]), a[2] if len(a) > 2 else None).run()
        [print(f"{k:<10}{v}") for k, v in r.items()]
    else: print(__doc__)
