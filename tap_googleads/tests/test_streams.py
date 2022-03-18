from tap_googleads.streams import flatten


def test_flatten():
    input_row = {
        "adGroup": {
            "resourceName": "string",
            "name": "string",
        },
        "metrics": {
            "clicks": "string",
            "costMicros": 2,
        },
        "adGroupCriterion": {
            "resourceName": "string",
            "ageRange": {
                    "type": "string",
            }
        },
        "ageRangeView": {
            "other": "string",
        },
        "segments": {
            "device": "string",
            "date": "string",
        }
    }
    res = flatten(
        input_row,
        keys_w_prefix=["adGroup", "metrics"],
        keys_no_prefix=["adGroupCriterion", "ageRangeView"]
    )

    assert res == {
        "adGroup_resourceName": "string",
        "adGroup_name": "string",
        "metrics_clicks": "string",
        "metrics_costMicros": 2,
        "resourceName": "string",
        "ageRange": {
            "type": "string",
        },
        "other": "string",
        "segments": {
            "device": "string",
            "date": "string",
        }
    }

