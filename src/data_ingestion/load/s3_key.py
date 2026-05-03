import re
from dataclasses import dataclass
from datetime import datetime


@dataclass
class RedditS3Key:
    subreddit: str
    head: str
    tail: str
    date: str
    timestamp: float

    _PATTERN = re.compile(
        r'raw/reddit/(?P<subreddit>[^/]+)/(?P<date>[^/]+)/'
        r'h-(?P<head>[^-]+)-t-(?P<tail>[^-]+)-tm-(?P<timestamp>[\d.]+)\.json$'
    )
    _TEMPLATE = 'raw/reddit/{subreddit}/{date}/h-{head}-t-{tail}-tm-{timestamp}.json'

    def to_s3_key(self) -> str:
        return self._TEMPLATE.format(
            subreddit=self.subreddit,
            date=self.date,
            head=self.head,
            tail=self.tail,
            timestamp=self.timestamp,
        )

    @classmethod
    def from_s3_key(cls, key: str) -> 'RedditS3Key':
        match = cls._PATTERN.search(key)
        if not match:
            raise ValueError(f'Cannot parse S3 key: {key}')
        return cls(
            subreddit=match.group('subreddit'),
            date=match.group('date'),
            head=match.group('head'),
            tail=match.group('tail'),
            timestamp=float(match.group('timestamp')),
        )

    @classmethod
    def build(cls, subreddit: str, head: str, tail: str) -> 'RedditS3Key':
        now = datetime.now()
        return cls(
            subreddit=subreddit,
            head=head,
            tail=tail,
            date=now.strftime('%Y-%m-%d'),
            timestamp=now.timestamp(),
        )
