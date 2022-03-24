from tap_googleads.utils import replicate_pk_at_root


def test_replicate_pk_at_root_composite():
    d = {"foo": {"level": "1", "id": 10}, "bar": 15}
    pk = ["foo.id", "bar"]

    res = replicate_pk_at_root(d, pk)

    assert res == {
        "foo": {"level": "1", "id": 10},
        "_sdc_primary_key": "10:15",
        "bar": 15,
    }


def test_replicate_pk_at_root_simple():
    d = {"foo": {"level": "1", "id": 10}, "bar": 15}
    pk = ["bar"]

    res = replicate_pk_at_root(d, pk)

    assert res == {"foo": {"level": "1", "id": 10}, "_sdc_primary_key": "15", "bar": 15}


def test_replicate_pk_at_root_no_jsonpath():
    d = {"bar": 15}
    pk = None

    res = replicate_pk_at_root(d, pk)

    assert res == {"bar": 15}
