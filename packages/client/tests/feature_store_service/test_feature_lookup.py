from datetime import timedelta
from pathlib import Path

from databricks.ml_features.entities.feature_lookup import FeatureLookup
from dataplatform.client.feature_store_service.feature_lookup import (
    create_feature_lookup,
    get_feature_lookups,
    parse_feature_lookup_config,
)


def test_create_feature_lookup():
    """Tests the creation of a FeatureLookup object."""
    lookup = create_feature_lookup(
        table_name="test_store",
        lookup_key="key",
        feature_names=["feature1", "feature2"],
        timestamp_lookup_key="ts_key",
        rename_outputs={"feature1": "f1_new"},
        lookback_window=timedelta(days=1),
        default_values={"feature2": 0},
    )
    assert isinstance(lookup, FeatureLookup)
    assert lookup.table_name == "test_store"
    assert lookup.lookup_key == "key"
    assert lookup.feature_names == ["feature1", "feature2"]
    assert lookup.timestamp_lookup_key == "ts_key"
    assert lookup._rename_outputs == {"feature1": "f1_new"}
    assert lookup.lookback_window == timedelta(days=1)
    assert lookup.default_values == {"feature2": 0}


def test_get_feature_lookups():
    """Tests yielding a generator of FeatureLookup objects from a config dictionary."""
    config = {
        "feature_lookups": [
            {
                "feature_store_name": "store1",
                "lookup_key": "key1",
                "feature_names": ["f1", "f2"],
            },
            {
                "feature_store_name": "store2",
                "lookup_key": "key2",
                "feature_names": ["f3"],
                "rename_outputs": {"f3": "feature3"},
            },
        ]
    }
    lookups = list(get_feature_lookups(config))
    assert len(lookups) == 2
    assert isinstance(lookups[0], FeatureLookup)
    assert lookups[0].table_name == "store1"
    assert lookups[0]._rename_outputs == {}
    assert isinstance(lookups[1], FeatureLookup)
    assert lookups[1].table_name == "store2"
    assert lookups[1]._rename_outputs == {"f3": "feature3"}


def test_parse_feature_lookup_config():
    """Tests parsing a feature lookup config file."""
    current_dir = Path(__file__).parent
    config_path = current_dir / "data" / "feature_lookup.yml"

    lookups = list(parse_feature_lookup_config(str(config_path)))

    assert len(lookups) == 1
    lookup = lookups[0]
    assert isinstance(lookup, FeatureLookup)
    assert lookup.table_name == "feature_store.member.member_features"
    assert lookup.lookup_key == "member_id"
    assert lookup.feature_names == [
        "checkin_last_30_days",
        "age_group",
        "membership_type",
    ]
    assert lookup.timestamp_lookup_key == "created_at"
    assert lookup._rename_outputs == {"age_group": "demographic_age_group"}
