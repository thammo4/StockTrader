#
# FILE: `StockTrader/infrastructure/redis/job_schema.py`
#

"""
Defines job schema for pricing computation task queue.
Serializes jobs as JSON --xmits-> Redis lists
"""

from dataclasses import dataclass, asdict
from typing import Optional
from datetime import datetime
import json


@dataclass
class PricingJob:
    batch_id: str
    market_date: str
    shard: int

    job_id: Optional[str] = None
    created_at: Optional[str] = None
    priority: int = 0

    prefixS3_input: Optional[str] = None
    prefixS3_output: Optional[str] = None

    def __post_init__(self):
        if self.job_id is None:
            self.job_id = f"{self.batch_id}_{self.market_date}_{self.shard}"
        if self.created_at is None:
            self.created_at = datetime.utcnow().isoformat()
        if self.prefixS3_input is None:
            self.prefixS3_input = f"batch_{self.batch_id}/market_date={self.market_date}/shard={self.shard}/"
        if self.prefixS3_output is None:
            self.prefixS3_output = f"batch_{self.batch_id}/market_date={self.market_date}/shard={self.shard}/"

    def to_json(self) -> str:
        return json.dumps(asdict(self))

    @classmethod
    def from_json(cls, json_str: str) -> "PricingJob":
        data = json.loads(json_str)
        return cls(**data)


Q_PENDING = "pricing:jobs:pending"
Q_PROCESSING = "pricing:jobs:processing"
Q_COMPLETED = "pricing:jobs:completed"
Q_FAILED = "pricing:jobs:failed"

WORKER_HEARTBEAT = "pricing:workers"
