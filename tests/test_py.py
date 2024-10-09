import subprocess
import time
import requests
import pytest

@pytest.fixture(scope="module", autouse=True)
def run_consumer_and_producer():
    consumer_process = subprocess.Popen(['python', '/scripts/consumer_v2.py'])
    time.sleep(5)
    producer_process = subprocess.Popen(['python', '/scripts/producer_v2.py'])
    time.sleep(5)
    yield
    producer_process.terminate()
    consumer_process.terminate()

def test_producer_consumer_interaction():
    response = requests.post("http://localhost:8000/send-message/", json={
        "tx_datetime": 1234567890,
        "tx_amount": 100.50,
        "tx_time_seconds": 30,
        "tx_time_days": 15,
        "avg_transaction_count_1": 5,
        "avg_transaction_mean_1": 50.0,
        "avg_transaction_count_7": 10,
        "avg_transaction_mean_7": 55.0,
        "avg_transaction_count_30": 20,
        "avg_transaction_mean_30": 60.0,
        "avg_transaction_terminal_id_count_1": 3,
        "avg_transaction_terminal_id_count_7": 4,
        "avg_transaction_terminal_id_count_30": 5
    })

    assert response.status_code == 200
    response_data = response.json()
    assert any("prediction" in item for item in response_data)

if __name__ == "__main__":
    pytest.main()
