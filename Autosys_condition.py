"""
AutoSys `condition:` (depends-on) -> Airflow 3.0 gate task.

Usage in a DAG:

    from autosys_condition import AutosysCondition

    gate = AutosysCondition(
        task_id="job_c__cond",
        condition="s(job_a) & (s(job_b) | f(job_x)) & e(job_d) = 0 & v(RUN_FLAG) = 'Y'",
        job_map={                      # AutoSys job -> (dag_id, task_id) in Airflow
            "job_a": ("dag_1", "job_a"),
            "job_b": ("dag_1", "job_b"),
            "job_x": ("dag_2", "job_x"),   # other DAG -> looked up via REST API
            "job_d": ("dag_1", "job_d"),
        },
        api_conn_id="airflow_api",     # needed only if some job is in another DAG
    )
    gate >> job_c

Supported:  s/success  f/failure  d/done  t/terminated  n/notrunning
            e/exitcode(JOB) <op> N   v/value(GLOBAL) <op> VALUE
            & | AND OR ( )   look-back: s(JOB, 03.30)   JOB^INS (instance ignored)
AND binds tighter than OR.

Per-job result of every check -> task log + XCom key "condition_lookup".
Exit code / terminated need the upstream task to push XCom "exit_code" /
"autosys_status"="TE" (see push_exit_code at the bottom).
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from functools import cached_property
from typing import Optional

from airflow.exceptions import AirflowSkipException
from airflow.sdk import BaseHook, BaseSensorOperator, Variable

# ============================================================== status mapping
SU, FA, TE, RU, NR = "SUCCESS", "FAILURE", "TERMINATED", "RUNNING", "NOT RUN"

AIRFLOW_TO_AUTOSYS = {
    "success": SU,
    "failed": FA,             # -> TE if XCom autosys_status == "TE"
    "upstream_failed": FA,
    "running": RU, "queued": RU, "scheduled": RU, "up_for_retry": RU,
    "up_for_reschedule": RU, "deferred": RU, "restarting": RU,
    # skipped / removed / None -> NOT RUN
}
TERMINAL = {"success", "failed", "upstream_failed", "skipped", "removed"}

# ============================================================== parser
FUNCS = {"s": "s", "success": "s", "f": "f", "failure": "f", "d": "d", "done": "d",
         "t": "t", "terminated": "t", "n": "n", "notrunning": "n",
         "e": "e", "exitcode": "e", "v": "v", "value": "v"}

TOKEN = re.compile(r"""
      (?P<ws>\s+) | (?P<lp>\() | (?P<rp>\)) | (?P<comma>,)
    | (?P<and>&&|&|\band\b) | (?P<or>\|\||\||\bor\b)
    | (?P<op>!=|<>|<=|>=|==|=|<|>)
    | (?P<str>"[^"]*"|'[^']*')
    | (?P<word>[^\s(),&|!=<>"']+)
""", re.X | re.I)


@dataclass(frozen=True)
class Atom:
    func: str
    name: str
    lookback: Optional[timedelta] = None
    op: Optional[str] = None
    value: Optional[str] = None


@dataclass(frozen=True)
class And:
    items: tuple


@dataclass(frozen=True)
class Or:
    items: tuple


def parse(text: str):
    toks = [(m.lastgroup, m.group()) for m in TOKEN.finditer(text) if m.lastgroup != "ws"]
    if "".join(v for _, v in toks).replace(" ", "") != re.sub(r"\s+", "", text):
        raise ValueError(f"Bad characters in condition: {text!r}")
    pos = 0

    def peek():
        return toks[pos][0] if pos < len(toks) else None

    def take(*kinds):
        nonlocal pos
        if pos >= len(toks) or (kinds and toks[pos][0] not in kinds):
            got = toks[pos][1] if pos < len(toks) else "end"
            raise ValueError(f"Expected {kinds} but got {got!r} in {text!r}")
        pos += 1
        return toks[pos - 1][1]

    def expr():
        items = [term()]
        while peek() == "or":
            take("or"); items.append(term())
        return items[0] if len(items) == 1 else Or(tuple(items))

    def term():
        items = [factor()]
        while peek() == "and":
            take("and"); items.append(factor())
        return items[0] if len(items) == 1 else And(tuple(items))

    def factor():
        if peek() == "lp":
            take("lp"); node = expr(); take("rp"); return node
        fname = take("word")
        func = FUNCS.get(fname.lower())
        if not func:
            raise ValueError(f"Unknown function {fname!r} in {text!r}")
        take("lp")
        name = take("word", "str").strip("\"'").split("^")[0]
        lookback = None
        if peek() == "comma":
            take("comma")
            m = re.fullmatch(r"(\d+)(?:[.:](\d{1,2}))?", take("word"))
            if not m:
                raise ValueError(f"Bad look-back in {text!r}")
            lookback = timedelta(hours=int(m[1]), minutes=int(m[2] or 0))
        take("rp")
        op = value = None
        if func in ("e", "v"):
            op = {"==": "=", "<>": "!="}.get(take("op"), toks[pos - 1][1])
            value = take("word", "str").strip("\"'")
        return Atom(func, name, lookback, op, value)

    node = expr()
    if pos != len(toks):
        raise ValueError(f"Unexpected {toks[pos][1]!r} in {text!r}")
    return node


def atoms(node):
    if isinstance(node, Atom):
        yield node
    else:
        for i in node.items:
            yield from atoms(i)


# ============================================================== evaluation
def compare(left, op, right) -> bool:
    if left is None:
        return False
    try:
        a, b = float(left), float(right)
    except (TypeError, ValueError):
        a, b = str(left), str(right)
    return {"=": a == b, "!=": a != b, "<": a < b, ">": a > b, "<=": a <= b, ">=": a >= b}[op]


def evaluate(node, results: dict) -> bool:
    """results: job -> {"state": SU/FA/TE/RU/NR, "exit_code": int|None}; "v(NAME)" -> {"value": ...}"""
    if isinstance(node, And):
        return all(evaluate(i, results) for i in node.items)
    if isinstance(node, Or):
        return any(evaluate(i, results) for i in node.items)
    if node.func == "v":
        return compare(results[f"v({node.name})"]["value"], node.op, node.value)
    r = results[node.name]
    st = r["state"]
    if node.func == "e":
        return r["exit_code"] is not None and compare(r["exit_code"], node.op, node.value)
    return {
        "s": st == SU,
        "f": st == FA,
        "t": st == TE,
        "d": st in (SU, FA, TE),
        "n": st != RU,
    }[node.func]


# ============================================================== REST (other DAGs)
class AirflowApi:
    """HTTP connection: host=https://your-airflow, login/password = read-only API user."""

    def __init__(self, conn_id: str, runs_to_scan: int = 10):
        self.conn_id, self.runs_to_scan = conn_id, runs_to_scan

    @cached_property
    def session(self):
        import requests
        c = BaseHook.get_connection(self.conn_id)
        base = c.host.rstrip("/")
        if not base.startswith("http"):
            base = f"{c.schema or 'https'}://{base}" + (f":{c.port}" if c.port else "")
        s = requests.Session()
        tok = s.post(f"{base}/auth/token", json={"username": c.login, "password": c.password}, timeout=30)
        tok.raise_for_status()
        s.headers["Authorization"] = f"Bearer {tok.json()['access_token']}"
        s.base = base
        return s

    def get(self, path, **params):
        r = self.session.get(f"{self.session.base}/api/v2{path}", params=params, timeout=30)
        if r.status_code == 404:
            return None
        r.raise_for_status()
        return r.json()

    def latest_ti(self, dag_id, task_id):
        """Newest run where the task has a state (AutoSys 'last status' semantics)."""
        runs = self.get(f"/dags/{dag_id}/dagRuns", order_by="-start_date", limit=self.runs_to_scan) or {}
        for run in runs.get("dag_runs", []):
            ti = self.get(f"/dags/{dag_id}/dagRuns/{run['dag_run_id']}/taskInstances/{task_id}")
            if ti and ti.get("state"):
                end = ti.get("end_date")
                end = datetime.fromisoformat(end.replace("Z", "+00:00")) if end else None
                return ti["state"], run["dag_run_id"], end
        return None, None, None

    def xcom(self, dag_id, task_id, run_id, key):
        x = self.get(f"/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}/xcomEntries/{key}")
        return x.get("value") if x else None


# ============================================================== the gate
class AutosysCondition(BaseSensorOperator):
    template_fields = ()
    ui_color = "#efe6fb"

    def __init__(self, *, condition: str, job_map: dict, api_conn_id: Optional[str] = None,
                 variable_prefix: str = "autosys_", **kwargs):
        kwargs.setdefault("mode", "reschedule")
        kwargs.setdefault("poke_interval", 60)
        kwargs.setdefault("soft_fail", True)          # timeout -> skipped (job never starts)
        super().__init__(**kwargs)
        parse(condition)                              # fail at DAG parse on bad syntax
        self.condition = condition
        self.job_map = {k: tuple(v) for k, v in job_map.items()}
        self.api_conn_id = api_conn_id
        self.variable_prefix = variable_prefix

    # ---------------------------------------------------------------- lookups
    def _lookup_job(self, ti, run_id, run_start, atom: Atom, api) -> dict:
        if atom.name not in self.job_map:
            return {"state": NR, "exit_code": None, "airflow_state": None,
                    "source": "job not in job_map"}
        dag_id, task_id = self.job_map[atom.name]

        if dag_id == ti.dag_id:                                    # same DAG -> this run
            res = ti.get_task_states(dag_id=dag_id, task_ids=[task_id], run_ids=[run_id])
            res = getattr(res, "task_states", res) or {}
            af_state, src_run, end, where = (res.get(run_id) or {}).get(task_id), run_id, None, "this run"
        else:                                                      # other DAG -> latest run
            if api is None:
                raise RuntimeError(f"{atom.name} is in DAG {dag_id}: set api_conn_id")
            af_state, src_run, end = api.latest_ti(dag_id, task_id)
            where = f"latest run {src_run}"

        af_state = None if af_state is None else str(getattr(af_state, "value", af_state))
        state = AIRFLOW_TO_AUTOSYS.get(af_state, NR)

        # look-back window (other DAGs only; 0 = since this run started)
        if atom.lookback is not None and dag_id != ti.dag_id and state in (SU, FA) and end:
            since = run_start if atom.lookback == timedelta(0) else datetime.now(timezone.utc) - atom.lookback
            if end < since:
                state, where = NR, f"{where}, ended {end} before look-back {since}"

        def xcom(key):
            try:
                if dag_id == ti.dag_id:
                    return ti.xcom_pull(task_ids=task_id, key=key)
                return api.xcom(dag_id, task_id, src_run, key)
            except Exception:
                return None

        exit_code = xcom("exit_code") if src_run and state in (SU, FA) else None
        if state == FA and xcom("autosys_status") == "TE":
            state = TE
        return {"state": state, "exit_code": None if exit_code is None else int(exit_code),
                "airflow_state": af_state, "source": f"{dag_id}.{task_id} ({where})"}

    # ---------------------------------------------------------------- poke
    def poke(self, context) -> bool:
        ti, run_id = context["ti"], context["run_id"]
        run_start = context["dag_run"].start_date
        if isinstance(run_start, str):
            run_start = datetime.fromisoformat(run_start.replace("Z", "+00:00"))
        api = AirflowApi(self.api_conn_id) if self.api_conn_id else None
        tree = parse(self.condition)

        # look up EVERY job so the report shows exactly which succeeded / failed
        results: dict = {}
        for a in atoms(tree):
            if a.func == "v":
                results[f"v({a.name})"] = {"value": Variable.get(self.variable_prefix + a.name, default=None)}
            elif a.name not in results:
                results[a.name] = self._lookup_job(ti, run_id, run_start, a, api)

        ok = evaluate(tree, results)

        lines = [f"condition: {self.condition}  =>  {ok}"]
        for job, r in results.items():
            if "value" in r:
                lines.append(f"  {job:<40} = {r['value']!r}")
            else:
                ec = "" if r["exit_code"] is None else f" exit={r['exit_code']}"
                lines.append(f"  {job:<40} -> {r['state']}{ec}  [{r['source']}]")
        self.log.info("\n".join(lines))
        ti.xcom_push(key="condition_lookup", value={"condition": self.condition, "result": ok, "jobs": results})

        if ok:
            return True
        # all referenced jobs are in this run and finished -> condition can never change
        same_run = [r for j, r in results.items() if "value" not in r]
        if (same_run and all(j in self.job_map and self.job_map[j][0] == ti.dag_id
                             for j in results if not j.startswith("v("))
                and all(r["airflow_state"] in TERMINAL for r in same_run)
                and not any(j.startswith("v(") for j in results)):
            raise AirflowSkipException("Condition can never be met in this run")
        return False


# ============================================================== helper for upstream tasks
def push_exit_code(context, exit_code: int, terminated: bool = False):
    """Call from your job task so e(JOB) and t(JOB) can be evaluated."""
    context["ti"].xcom_push(key="exit_code", value=exit_code)
    if terminated:
        context["ti"].xcom_push(key="autosys_status", value="TE")
