# test_te_poller.py

import os
import pytest
from unittest.mock import patch
import main as app   # make sure this matches your main app filename

def fake_list(*args, **kwargs):
    return {"response": {"list": [{"id": "1"}]}}

def fake_detail(*args, **kwargs):
    # must match your CUSTOMER_FILTERS keywords, here "vicky"
    return {"response": [{"initiation": {"loc": {"name": "Vicky Test"}}}]}

def fake_tracking_old(*args, **kwargs):
    return {
        "response": [
            {
                "id": "1",
                "number": "ABC",
                "list": [
                    {
                        "timestamp": "1000",
                        "location": "",
                        "context": "processing",
                        "datetime": {"America/Vancouver": "2025-05-23 10:00"}
                    }
                ]
            }
        ]
    }

def fake_tracking_new(*args, **kwargs):
    return {
        "response": [
            {
                "id": "1",
                "number": "ABC",
                "list": [
                    {
                        "timestamp": "1000",
                        "location": "",
                        "context": "processing",
                        "datetime": {"America/Vancouver": "2025-05-23 10:00"}
                    },
                    {
                        "timestamp": "2000",
                        "location": "Loc",
                        "context": "delivered",
                        "datetime": {"America/Vancouver": "2025-05-23 11:00"}
                    }
                ]
            }
        ]
    }

@pytest.fixture(autouse=True)
def clean_state(tmp_path, monkeypatch):
    # Redirect STATE_FILE to a temp path so tests don't share real state
    state_file = tmp_path / "last_seen.json"
    monkeypatch.setenv("STATE_FILE", str(state_file))
    app.STATE_FILE = str(state_file)
    yield

@patch("main.call_api")
@patch("main.requests.post")
def test_poller_pushes_only_on_change(mock_post, mock_call_api):
    # Simulate: first poll returns old tracking; second poll returns new tracking
    mock_call_api.side_effect = [
        fake_list(), fake_detail(), fake_tracking_old(),
        fake_list(), fake_detail(), fake_tracking_new()
    ]

    # First invocation → seeds state → pushes once
    app.check_te_updates()
    assert mock_post.call_count == 1, f"Expected 1 push on first poll, got {mock_post.call_count}"

    # Second invocation → sees new timestamp → pushes again (total 2)
    app.check_te_updates()
    assert mock_post.call_count == 2, f"Expected 2 pushes after second poll, got {mock_post.call_count}"

    # The second push (call #2) should include "Loc"
    # call_args_list[1] is the args/kwargs of the second call
    _, kwargs_second = mock_post.call_args_list[1]
    pushed = kwargs_second["json"]["messages"][0]["text"]
    assert "Loc" in pushed, f"Expected 'Loc' in second push, got: {pushed}"
