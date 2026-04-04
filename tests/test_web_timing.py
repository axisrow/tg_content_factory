from src.web.timing import RequestProfiler, TimingBuffer, TimingRecord, get_current_profiler


def test_get_current_profiler_default_none():
    assert get_current_profiler() is None


def test_profiler_activate_sets_context():
    p = RequestProfiler()
    p.activate()
    assert get_current_profiler() is p
    p.deactivate()


def test_profiler_deactivate_clears_context():
    p = RequestProfiler()
    p.activate()
    p.deactivate()
    assert get_current_profiler() is None


def test_profiler_deactivate_idempotent():
    p = RequestProfiler()
    p.activate()
    p.deactivate()
    p.deactivate()
    assert get_current_profiler() is None


def test_profiler_record_db():
    p = RequestProfiler()
    p.record_db(5_000_000)
    p.record_db(3_000_000)
    assert p.db_ns == 8_000_000
    assert p.db_queries == 2


def test_profiler_to_breakdown():
    p = RequestProfiler()
    p.record_db(1_500_000)
    p.record_db(2_500_000)
    b = p.to_breakdown()
    assert b["db_ms"] == 4
    assert b["db_queries"] == 2


def test_timing_buffer_add_and_get():
    buf = TimingBuffer(maxlen=5)
    for i in range(7):
        buf.add(TimingRecord(time=f"00:00:0{i}", method="GET", path="/", status=200, ms=i))
    records = buf.get_records()
    assert len(records) == 5
    assert records[0]["ms"] == 2
    assert records[-1]["ms"] == 6


def test_timing_buffer_default_maxlen():
    buf = TimingBuffer()
    assert buf.get_records() == []
