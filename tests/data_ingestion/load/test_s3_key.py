import pytest
from data_ingestion.load.s3_key import RedditS3Key


VALID_KEY = 'raw/reddit/Bitcoin/2026-05-03/h-t3_abc123-t-t3_xyz789-tm-1746230400.0.json'


# ============================================
# ---------- Tests for RedditS3Key -----------
# ============================================


def test_from_s3_key_parses_all_fields():
    key = RedditS3Key.from_s3_key(VALID_KEY)

    assert key.subreddit == 'Bitcoin'
    assert key.date == '2026-05-03'
    assert key.head == 't3_abc123'
    assert key.tail == 't3_xyz789'
    assert key.timestamp == 1746230400.0


def test_from_s3_key_raises_on_invalid_key():
    with pytest.raises(ValueError, match='Cannot parse S3 key'):
        RedditS3Key.from_s3_key('raw/reddit/Bitcoin/some-weird-file.json')


def test_to_s3_key_roundtrip():
    """Parsing and re-serializing a key should return the original string."""
    key_obj = RedditS3Key.from_s3_key(VALID_KEY)
    assert key_obj.to_s3_key() == VALID_KEY


def test_build_produces_valid_parseable_key():
    key_obj = RedditS3Key.build(subreddit='Ethereum', head='t3_head1', tail='t3_tail1')
    s3_key = key_obj.to_s3_key()

    parsed = RedditS3Key.from_s3_key(s3_key)
    assert parsed.subreddit == 'Ethereum'
    assert parsed.head == 't3_head1'
    assert parsed.tail == 't3_tail1'


def test_build_sets_current_date(freezegun_or_mock=None):
    from unittest.mock import patch
    from datetime import datetime

    fixed_dt = datetime(2026, 5, 3, 12, 0, 0)
    with patch('data_ingestion.load.s3_key.datetime') as mock_dt:
        mock_dt.now.return_value = fixed_dt
        key_obj = RedditS3Key.build('Bitcoin', 't3_h', 't3_t')

    assert key_obj.date == '2026-05-03'
    assert key_obj.timestamp == fixed_dt.timestamp()
