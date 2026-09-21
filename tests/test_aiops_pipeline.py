from pathlib import Path

from src.anomaly_detector import AnomalyDetector
from src.aiops_pipeline import run_pipeline
from src.event_consumer import EventConsumer
from src.event_producer import EventProducer
from src.event_topic import EventTopic


def test_normal_record_is_not_anomaly():
    detector = AnomalyDetector()

    record = {
        "timestamp": "2026-09-20T10:00:00",
        "service": "payment-service",
        "response_time_ms": 120,
        "cpu_percent": 42,
        "memory_percent": 51,
        "log_level": "INFO",
        "message": "Payment request processed successfully"
    }

    assert detector.detect(record) is None


def test_anomalous_record_is_detected():
    detector = AnomalyDetector()

    record = {
        "timestamp": "2026-09-20T10:05:00",
        "service": "payment-service",
        "response_time_ms": 610,
        "cpu_percent": 75,
        "memory_percent": 70,
        "log_level": "ERROR",
        "message": "Payment service timeout"
    }

    event = detector.detect(record)

    assert event is not None
    assert event["type"] == "ANOMALY"


def test_detector_reports_each_anomaly_reason():
    detector = AnomalyDetector()

    record = {
        "timestamp": "2026-09-20T10:05:00",
        "service": "payment-service",
        "response_time_ms": 501,
        "cpu_percent": 81,
        "memory_percent": 81,
        "log_level": "WARNING",
        "message": "Service warning",
    }

    event = detector.detect(record)

    assert event["reasons"] == [
        "High response time",
        "High CPU utilization",
        "High memory utilization",
        "Error log detected",
    ]


def test_producer_publishes_event():
    topic = EventTopic("anomaly-events")
    producer = EventProducer(topic)

    event = {
        "type": "ANOMALY",
        "service": "payment-service"
    }

    assert producer.publish(event)
    assert len(topic.get_messages()) == 1


def test_producer_rejects_empty_event():
    producer = EventProducer(EventTopic("anomaly-events"))

    assert producer.publish(None) is False


def test_topic_clear_removes_all_messages():
    topic = EventTopic("anomaly-events")

    topic.publish({"type": "ANOMALY"})
    topic.publish({"type": "ANOMALY"})
    assert len(topic.get_messages()) == 2

    topic.clear()

    assert topic.get_messages() == []


def test_consumer_receives_event():
    topic = EventTopic("anomaly-events")
    producer = EventProducer(topic)
    consumer = EventConsumer(topic)

    event = {
        "type": "ANOMALY",
        "service": "payment-service"
    }

    producer.publish(event)

    messages = consumer.consume()

    assert len(messages) == 1


def test_consumer_returns_empty_topic():
    consumer = EventConsumer(EventTopic("anomaly-events"))

    assert consumer.consume() == []


def test_pipeline_processes_records_and_consumes_anomalies(tmp_path):
    data_file = tmp_path / "service_data.json"
    data_file.write_text(
        "[{\"timestamp\": \"2026-09-20T10:00:00\", "
        "\"service\": \"payment-service\", "
        "\"response_time_ms\": 601, \"cpu_percent\": 42, "
        "\"memory_percent\": 51, \"log_level\": \"INFO\", "
        "\"message\": \"Timeout\"}]",
        encoding="utf-8",
    )

    result = run_pipeline(data_file)

    assert result["records_processed"] == 1
    assert len(result["anomalies_detected"]) == 1
    assert result["events_consumed"] == []


def test_pipeline_handles_records_without_anomalies(tmp_path):
    data_file = tmp_path / "service_data.json"
    data_file.write_text(
        "[{\"timestamp\": \"2026-09-20T10:00:00\", "
        "\"service\": \"payment-service\", "
        "\"response_time_ms\": 120, \"cpu_percent\": 42, "
        "\"memory_percent\": 51, \"log_level\": \"INFO\", "
        "\"message\": \"OK\"}]",
        encoding="utf-8",
    )

    result = run_pipeline(data_file)

    assert result == {
        "records_processed": 1,
        "anomalies_detected": [],
        "events_consumed": [],
    }
