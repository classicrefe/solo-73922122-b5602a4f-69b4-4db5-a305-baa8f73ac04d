"""可选上传编号（幂等键）的测试。

覆盖：同编号同字节返回首次回执、同编号不同字节拒绝、失败不占用编号、
缺件不返回成功回执、结构变化 / 转换期间改列，以及双实例并发至多登记一批。
"""

from __future__ import annotations

import sqlite3
import threading
from pathlib import Path

import httpx
import pytest
from fastapi.testclient import TestClient

from app import lake
from app.lake import Settings
from app.main import create_app

from test_app import (
    add,
    current,
    dual_instances,
    file_digest,
    simple_csv,
)


@pytest.fixture()
def settings(tmp_path: Path) -> Settings:
    s = Settings(data_dir=tmp_path / "lake", instance_name="test")
    lake.initialize(s)
    return s


@pytest.fixture()
def client(settings: Settings):
    with TestClient(create_app(settings)) as c:
        yield c


def upload(
    client: TestClient,
    data: bytes,
    *,
    version: int | None = None,
    upload_id: str | None = None,
    filename: str = "a.csv",
):
    if version is None:
        version = current(client)["schema"]["schema_version"]
    form: dict[str, str] = {"version": str(version)}
    if upload_id is not None:
        form["upload_id"] = upload_id
    return client.post(
        "/api/batches",
        data=form,
        files={"file": (filename, data, "text/csv")},
    )


def parquet_names(settings: Settings) -> list[str]:
    return sorted(p.name for p in settings.parquet_dir.glob("*.parquet"))


# ---------------------------------------------------------------------------
# API：回执重放与冲突
# ---------------------------------------------------------------------------


def test_same_id_same_bytes_returns_first_receipt(client: TestClient, settings: Settings):
    add(client, "temp_c")
    v = current(client)["schema"]["schema_version"]
    payload = simple_csv(temp_c=7)

    first = upload(client, payload, version=v, upload_id="station-1-20260101")
    assert first.status_code == 201, first.text
    body = first.json()
    assert body["upload_id"] == "station-1-20260101"
    assert body["replayed"] is False
    path = settings.parquet_dir / body["file_name"]
    before = file_digest(path)

    retry = upload(client, payload, version=v, upload_id="station-1-20260101")
    assert retry.status_code == 200, retry.text
    replay = retry.json()
    assert replay["id"] == body["id"]
    assert replay["replayed"] is True
    assert replay["created_at"] == body["created_at"]
    assert replay["file_sha256"] == body["file_sha256"]

    state = current(client)
    assert [b["id"] for b in state["batches"]] == [body["id"]]
    assert len(parquet_names(settings)) == 1
    assert file_digest(path) == before, "重放不得改写首次写入的文件"


def test_different_bytes_same_id_rejected_and_original_unchanged(
    client: TestClient, settings: Settings
):
    add(client, "temp_c")
    v = current(client)["schema"]["schema_version"]
    first = upload(client, simple_csv(temp_c=1), version=v, upload_id="dup-1")
    assert first.status_code == 201, first.text
    original = first.json()
    path = settings.parquet_dir / original["file_name"]
    before = file_digest(path)
    state_before = current(client)

    conflict = upload(client, simple_csv(temp_c=2), version=v, upload_id="dup-1")
    assert conflict.status_code == 409, conflict.text
    payload = conflict.json()
    assert payload["error"] == "upload_id_conflict"
    assert payload["upload_id"] == "dup-1"
    assert payload["batch_id"] == original["id"]

    assert current(client) == state_before
    assert file_digest(path) == before
    assert len(parquet_names(settings)) == 1


def test_replay_wins_even_after_schema_changed(client: TestClient, settings: Settings):
    add(client, "temp_c")
    v1 = current(client)["schema"]["schema_version"]
    payload = simple_csv(temp_c=42)
    first = upload(client, payload, version=v1, upload_id="late-receipt")
    assert first.status_code == 201, first.text
    batch = first.json()
    path = settings.parquet_dir / batch["file_name"]
    before = file_digest(path)

    field_id = current(client)["schema"]["fields"][0]["id"]
    renamed = client.patch(
        f"/api/fields/{field_id}", json={"schema_version": v1, "name": "temp_air"}
    )
    assert renamed.status_code == 200
    assert renamed.json()["schema_version"] == v1 + 1

    # 客户端仍拿着旧版本 v1 重试：先命中编号，必须返回首次回执而不做结构校验。
    retry = upload(client, payload, version=v1, upload_id="late-receipt")
    assert retry.status_code == 200, retry.text
    assert retry.json()["id"] == batch["id"]
    assert retry.json()["replayed"] is True
    assert retry.json()["schema_version"] == v1

    state = current(client)
    assert len(state["batches"]) == 1
    assert file_digest(path) == before

    view = client.get(f"/api/batches/{batch['id']}").json()
    assert view["columns"] == ["station_id", "observed_at", "temp_air"]
    assert view["rows"][0][2] == "42"


def test_no_id_keeps_original_behavior(client: TestClient, settings: Settings):
    add(client, "temp_c")
    v = current(client)["schema"]["schema_version"]
    payload = simple_csv(temp_c=3)

    one = upload(client, payload, version=v)
    two = upload(client, payload, version=v)
    assert one.status_code == two.status_code == 201
    assert one.json()["id"] != two.json()["id"]
    assert one.json()["upload_id"] is None
    assert one.json()["replayed"] is False
    assert len(current(client)["batches"]) == 2
    assert len(parquet_names(settings)) == 2


def test_blank_id_is_treated_as_absent(client: TestClient):
    add(client, "temp_c")
    v = current(client)["schema"]["schema_version"]
    one = upload(client, simple_csv(temp_c=3), version=v, upload_id="   ")
    two = upload(client, simple_csv(temp_c=3), version=v, upload_id="")
    assert one.status_code == two.status_code == 201
    assert one.json()["upload_id"] is None
    assert one.json()["id"] != two.json()["id"]


@pytest.mark.parametrize("bad_id", ["x" * 129, "bad\x00id", "line\nbreak"])
def test_invalid_id_rejected(client: TestClient, settings: Settings, bad_id: str):
    add(client, "temp_c")
    resp = upload(client, simple_csv(temp_c=1), upload_id=bad_id)
    assert resp.status_code == 400, resp.text
    assert current(client)["batches"] == []
    assert parquet_names(settings) == []


# ---------------------------------------------------------------------------
# 失败路径：编号不被占用，缺件不返回回执
# ---------------------------------------------------------------------------


def test_stale_first_upload_does_not_occupy_id(client: TestClient, settings: Settings):
    add(client, "temp_c")
    v = current(client)["schema"]["schema_version"]
    payload = simple_csv(temp_c=5)

    stale = upload(client, payload, version=v - 1, upload_id="reuse-me")
    assert stale.status_code == 409
    assert current(client)["batches"] == []
    assert parquet_names(settings) == []

    # 编号未被登记，可用同一编号、按当前结构重试。
    retry = upload(client, payload, version=v, upload_id="reuse-me")
    assert retry.status_code == 201, retry.text
    assert retry.json()["upload_id"] == "reuse-me"
    assert len(current(client)["batches"]) == 1


def test_structure_change_during_conversion_keeps_id_reusable(
    client: TestClient, settings: Settings, monkeypatch
):
    add(client, "temp_c")
    v = current(client)["schema"]["schema_version"]
    payload = simple_csv(temp_c=6)

    def add_field_during_conversion() -> None:
        lake.add_field(settings, v, "humidity")

    monkeypatch.setattr(lake, "_before_commit_hook", add_field_during_conversion)
    try:
        stale = upload(client, payload, version=v, upload_id="retry-after-change")
    finally:
        monkeypatch.setattr(lake, "_before_commit_hook", None)
    assert stale.status_code == 409, stale.text
    assert current(client)["batches"] == []
    assert parquet_names(settings) == [], "过期上传不得留下文件"

    new_version = current(client)["schema"]["schema_version"]
    retry = upload(client, payload, version=new_version, upload_id="retry-after-change")
    assert retry.status_code == 201, retry.text
    assert retry.json()["upload_id"] == "retry-after-change"
    assert len(current(client)["batches"]) == 1


def test_registration_failure_leaves_id_reusable(
    client: TestClient, settings: Settings, monkeypatch
):
    add(client, "temp_c")
    v = current(client)["schema"]["schema_version"]
    payload = simple_csv(temp_c=8)
    real_commit = lake._commit_batch

    def fail_once(*args, **kwargs):
        raise lake.StorageError("登记失败（模拟）")

    monkeypatch.setattr(lake, "_commit_batch", fail_once)
    try:
        failed = upload(client, payload, version=v, upload_id="after-failure")
    finally:
        monkeypatch.setattr(lake, "_commit_batch", real_commit)
    assert failed.status_code == 500
    assert current(client)["batches"] == []
    assert parquet_names(settings) == []

    retry = upload(client, payload, version=v, upload_id="after-failure")
    assert retry.status_code == 201, retry.text
    assert retry.json()["replayed"] is False
    assert len(current(client)["batches"]) == 1


def test_crash_between_file_write_and_registration_leaves_id_reusable(
    client: TestClient, settings: Settings, monkeypatch
):
    """模拟进程在“文件已落盘、登记未提交”之间退出：编号不得被占用。"""

    add(client, "temp_c")
    v = current(client)["schema"]["schema_version"]
    payload = simple_csv(temp_c=9)
    real_commit = lake._commit_batch

    def crash(*args, **kwargs):
        raise SystemExit("simulated process exit")

    monkeypatch.setattr(lake, "_commit_batch", crash)
    monkeypatch.setattr(lake, "_safe_unlink", lambda path: None)
    try:
        with pytest.raises(SystemExit):
            lake.upload_csv(settings, v, payload, upload_id="crash-id")
    finally:
        monkeypatch.setattr(lake, "_commit_batch", real_commit)
        monkeypatch.setattr(lake, "_safe_unlink", lake._safe_unlink)

    # 崩溃后：有残留文件，但没有批次登记，编号仍可用。
    assert current(client)["batches"] == []
    retry = upload(client, payload, version=v, upload_id="crash-id")
    assert retry.status_code == 201, retry.text
    assert retry.json()["replayed"] is False
    assert len(current(client)["batches"]) == 1


def test_missing_file_is_not_returned_as_successful_receipt(
    client: TestClient, settings: Settings
):
    add(client, "temp_c")
    v = current(client)["schema"]["schema_version"]
    payload = simple_csv(temp_c=11)
    first = upload(client, payload, version=v, upload_id="missing-file")
    assert first.status_code == 201, first.text
    batch = first.json()
    (settings.parquet_dir / batch["file_name"]).unlink()

    retry = upload(client, payload, version=v, upload_id="missing-file")
    assert retry.status_code == 500, retry.text
    assert retry.json()["error"] == "integrity_error"
    assert "缺失" in retry.json()["detail"]
    assert len(current(client)["batches"]) == 1
    assert parquet_names(settings) == []


# ---------------------------------------------------------------------------
# 并发：同一编号至多登记一批（两个 Settings 共享目录，等价双实例）
# ---------------------------------------------------------------------------


def _run_concurrently(callables):
    barrier = threading.Barrier(len(callables))
    results: list[object] = [None] * len(callables)
    errors: list[BaseException] = [None] * len(callables)  # type: ignore[list-item]

    def run(index: int) -> None:
        barrier.wait()
        try:
            results[index] = callables[index]()
        except BaseException as exc:  # noqa: BLE001 - 测试需记录全部异常
            errors[index] = exc

    threads = [threading.Thread(target=run, args=(i,)) for i in range(len(callables))]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=30)
    return results, errors


def test_concurrent_same_id_same_bytes_registers_one_batch(settings: Settings):
    lake.add_field(settings, 0, "temp_c")
    version = lake.read_schema(settings).version
    payload = simple_csv(temp_c=21)
    node_a = Settings(data_dir=settings.data_dir, instance_name="node-a")
    node_b = Settings(data_dir=settings.data_dir, instance_name="node-b")

    results, errors = _run_concurrently(
        [
            lambda: lake.upload_csv(node_a, version, payload, upload_id="race-same"),
            lambda: lake.upload_csv(node_b, version, payload, upload_id="race-same"),
        ]
    )
    assert errors == [None, None], errors
    assert results[0].id == results[1].id
    assert sorted(bool(r.replayed) for r in results) == [False, True]
    assert len(lake.read_state(settings).batches) == 1
    assert len(parquet_names(settings)) == 1
    assert lake.check_integrity(settings).ok


def test_concurrent_same_id_different_bytes_allows_one(settings: Settings):
    lake.add_field(settings, 0, "temp_c")
    version = lake.read_schema(settings).version
    node_a = Settings(data_dir=settings.data_dir, instance_name="node-a")
    node_b = Settings(data_dir=settings.data_dir, instance_name="node-b")

    results, errors = _run_concurrently(
        [
            lambda: lake.upload_csv(
                node_a, version, simple_csv(temp_c=1), upload_id="race-diff"
            ),
            lambda: lake.upload_csv(
                node_b, version, simple_csv(temp_c=2), upload_id="race-diff"
            ),
        ]
    )
    conflicts = [e for e in errors if isinstance(e, lake.UploadIdConflict)]
    assert len(conflicts) == 1, errors
    winners = [r for r in results if r is not None]
    assert len(winners) == 1
    assert len(lake.read_state(settings).batches) == 1
    assert parquet_names(settings) == [winners[0].file_name]
    assert lake.check_integrity(settings).ok


# ---------------------------------------------------------------------------
# 网页：失败后保留编号
# ---------------------------------------------------------------------------


def test_web_failure_preserves_id_for_retry(client: TestClient, settings: Settings):
    add(client, "temp_c")
    v = current(client)["schema"]["schema_version"]
    payload = simple_csv(temp_c=12)

    failed = client.post(
        "/upload",
        data={"version": str(v - 1), "upload_id": "web-retry-1"},
        files={"file": ("a.csv", payload, "text/csv")},
        follow_redirects=False,
    )
    assert failed.status_code == 409, failed.text
    assert 'value="web-retry-1"' in failed.text
    assert "结构版本已过期" in failed.text
    # 失败页绑定当前已提交版本，用户可直接沿用编号重试。
    assert f'name="version" value="{v}"' in failed.text
    assert current(client)["batches"] == []

    retry = client.post(
        "/upload",
        data={"version": str(v), "upload_id": "web-retry-1"},
        files={"file": ("a.csv", payload, "text/csv")},
        follow_redirects=False,
    )
    assert retry.status_code == 303, retry.text
    batch_id = retry.headers["location"].split("/batches/")[1].split("?")[0]
    state = current(client)
    assert [b["id"] for b in state["batches"]] == [batch_id]
    assert state["batches"][0]["upload_id"] == "web-retry-1"

    again = client.post(
        "/upload",
        data={"version": str(v), "upload_id": "web-retry-1"},
        files={"file": ("a.csv", payload, "text/csv")},
        follow_redirects=True,
    )
    assert "未再次入湖" in again.text
    assert len(current(client)["batches"]) == 1


def test_web_upload_without_id_unchanged(client: TestClient):
    add(client, "temp_c")
    v = current(client)["schema"]["schema_version"]
    payload = simple_csv(temp_c=13)
    for _ in range(2):
        resp = client.post(
            "/upload",
            data={"version": str(v)},
            files={"file": ("a.csv", payload, "text/csv")},
            follow_redirects=False,
        )
        assert resp.status_code == 303
    assert len(current(client)["batches"]) == 2


def test_web_conflict_shows_error_and_keeps_id(client: TestClient):
    add(client, "temp_c")
    v = current(client)["schema"]["schema_version"]
    first = client.post(
        "/upload",
        data={"version": str(v), "upload_id": "web-dup"},
        files={"file": ("a.csv", simple_csv(temp_c=1), "text/csv")},
        follow_redirects=False,
    )
    assert first.status_code == 303
    conflict = client.post(
        "/upload",
        data={"version": str(v), "upload_id": "web-dup"},
        files={"file": ("b.csv", simple_csv(temp_c=2), "text/csv")},
        follow_redirects=False,
    )
    assert conflict.status_code == 409
    assert "已登记为批次" in conflict.text
    assert 'value="web-dup"' in conflict.text
    assert len(current(client)["batches"]) == 1


def test_web_blank_id_submitted_as_absent(client: TestClient):
    add(client, "temp_c")
    v = current(client)["schema"]["schema_version"]
    resp = client.post(
        "/upload",
        data={"version": str(v), "upload_id": "  "},
        files={"file": ("a.csv", simple_csv(temp_c=1), "text/csv")},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    batch = current(client)["batches"][0]
    assert batch["upload_id"] is None


# ---------------------------------------------------------------------------
# 兼容旧库：无 upload_id 列的数据库自动迁移
# ---------------------------------------------------------------------------


def test_initialize_migrates_legacy_database(tmp_path: Path):
    data_dir = tmp_path / "legacy"
    parquet_dir = data_dir / "parquet"
    parquet_dir.mkdir(parents=True)
    db_path = data_dir / "lake.sqlite3"
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
        INSERT INTO meta(key, value) VALUES ('schema_version', '0');
        CREATE TABLE fields (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            ordinal INTEGER NOT NULL,
            status TEXT NOT NULL CHECK (status IN ('active', 'deleted')),
            created_version INTEGER NOT NULL,
            deleted_version INTEGER,
            created_at TEXT NOT NULL,
            deleted_at TEXT
        );
        CREATE TABLE batches (
            id TEXT PRIMARY KEY,
            created_at TEXT NOT NULL,
            schema_version INTEGER NOT NULL,
            row_count INTEGER NOT NULL,
            file_name TEXT NOT NULL UNIQUE,
            file_sha256 TEXT NOT NULL,
            schema_json TEXT NOT NULL
        );
        INSERT INTO batches(id, created_at, schema_version, row_count, file_name,
                            file_sha256, schema_json)
        VALUES ('legacy-batch', '2026-01-01T00:00:00Z', 0, 1, 'legacy.parquet',
                'deadbeef', '[]');
        """
    )
    conn.commit()
    conn.close()

    settings = Settings(data_dir=data_dir, instance_name="migrated")
    lake.initialize(settings)
    with sqlite3.connect(db_path) as check:
        columns = {row[1] for row in check.execute("PRAGMA table_info(batches)")}
    assert {"upload_id", "upload_sha256"} <= columns

    state = lake.read_state(settings)
    assert [b.id for b in state.batches] == ["legacy-batch"]
    assert state.batches[0].upload_id is None

    schema = lake.add_field(settings, 0, "temp_c")
    batch = lake.upload_csv(
        settings, schema.version, simple_csv(temp_c=4), upload_id="post-migration"
    )
    assert batch.upload_id == "post-migration"
    replay = lake.upload_csv(
        settings, schema.version, simple_csv(temp_c=4), upload_id="post-migration"
    )
    assert replay.id == batch.id and replay.replayed


# ---------------------------------------------------------------------------
# 真实双进程（uvicorn x2，共享数据目录）
# ---------------------------------------------------------------------------


def test_two_processes_same_id_register_once(dual_instances):
    urls, data_dir = dual_instances
    a, b = urls

    created = httpx.post(f"{a}/api/fields", json={"schema_version": 0, "name": "temp_c"})
    assert created.status_code == 201, created.text
    payload = simple_csv(temp_c=33)
    results: list[httpx.Response] = []

    def attempt(url: str) -> None:
        results.append(
            httpx.post(
                f"{url}/api/batches",
                data={"version": "1", "upload_id": "dual-same"},
                files={"file": ("a.csv", payload, "text/csv")},
                timeout=30,
            )
        )

    left = threading.Thread(target=attempt, args=(a,))
    right = threading.Thread(target=attempt, args=(b,))
    left.start()
    right.start()
    left.join()
    right.join()

    assert sorted(r.status_code for r in results) == [200, 201], [
        r.text for r in results
    ]
    ids = {r.json()["id"] for r in results}
    assert len(ids) == 1
    assert sorted(r.json()["replayed"] for r in results) == [False, True]

    state_b = httpx.get(f"{b}/api/state").json()
    assert len(state_b["batches"]) == 1
    assert state_b["batches"][0]["upload_id"] == "dual-same"
    assert len(list((data_dir / "parquet").glob("*.parquet"))) == 1
    assert httpx.get(f"{b}/api/integrity").json()["ok"] is True

    # 回执丢失后重试：另一个实例返回同一批次的回执。
    again = httpx.post(
        f"{b}/api/batches",
        data={"version": "1", "upload_id": "dual-same"},
        files={"file": ("a.csv", payload, "text/csv")},
        timeout=30,
    )
    assert again.status_code == 200, again.text
    assert again.json()["id"] == ids.pop()
    assert again.json()["replayed"] is True

    conflict = httpx.post(
        f"{a}/api/batches",
        data={"version": "1", "upload_id": "dual-same"},
        files={"file": ("b.csv", simple_csv(temp_c=34), "text/csv")},
        timeout=30,
    )
    assert conflict.status_code == 409, conflict.text
    assert httpx.get(f"{b}/api/state").json()["batches"] == state_b["batches"]


# ---------------------------------------------------------------------------
# 编号的其他传递方式（查询参数 / 请求头 / upload_no 表单别名）
# ---------------------------------------------------------------------------


def test_upload_id_via_query_header_and_alias(client: TestClient):
    add(client, "temp_c")
    v = current(client)["schema"]["schema_version"]
    payload = simple_csv(temp_c=17)

    by_query = client.post(
        f"/api/batches?upload_id=via-query",
        data={"version": str(v)},
        files={"file": ("a.csv", payload, "text/csv")},
    )
    assert by_query.status_code == 201, by_query.text
    assert by_query.json()["upload_id"] == "via-query"

    by_header = client.post(
        "/api/batches",
        data={"version": str(v)},
        headers={"Idempotency-Key": "via-header"},
        files={"file": ("b.csv", simple_csv(temp_c=18), "text/csv")},
    )
    assert by_header.status_code == 201, by_header.text
    assert by_header.json()["upload_id"] == "via-header"

    by_alias = client.post(
        "/api/batches",
        data={"version": str(v), "upload_no": "via-alias"},
        files={"file": ("c.csv", simple_csv(temp_c=19), "text/csv")},
    )
    assert by_alias.status_code == 201, by_alias.text
    assert by_alias.json()["upload_id"] == "via-alias"

    replay = client.post(
        f"/api/batches?upload_id=via-query",
        data={"version": str(v), "upload_id": "via-query"},
        headers={"Idempotency-Key": "via-query"},
        files={"file": ("a.csv", payload, "text/csv")},
    )
    assert replay.status_code == 200, replay.text
    assert replay.json()["id"] == by_query.json()["id"]

    mismatch = client.post(
        f"/api/batches?upload_id=one",
        data={"version": str(v), "upload_id": "two"},
        files={"file": ("d.csv", simple_csv(temp_c=20), "text/csv")},
    )
    assert mismatch.status_code == 400, mismatch.text
    assert len(current(client)["batches"]) == 3
