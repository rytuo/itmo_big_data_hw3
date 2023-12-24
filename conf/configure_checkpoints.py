import os

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.checkpoint_config import CheckpointConfig, ExternalizedCheckpointCleanup
from pyflink.datastream.checkpointing_mode import CheckpointingMode
from pyflink.datastream.checkpoint_storage import FileSystemCheckpointStorage

from dotenv import load_dotenv

load_dotenv()

CHECKPOINT_DESTINATION=os.environ.get("CHECKPOINT_DESTINATION")
CHECKPOINT_TIME_MS=int(os.environ.get("CHECKPOINT_TIME_MS"))
CHECKPOINT_TIMEOUT_MS=int(os.environ.get("CHECKPOINT_TIMEOUT_MS"))
TOLERABLE_CHECKPOINT_FAILURE_NUMBER=int(os.environ.get("TOLERABLE_CHECKPOINT_FAILURE_NUMBER"))
MIN_PAUSE_BETWEEN_CHECKPOINTS_MS=int(os.environ.get("MIN_PAUSE_BETWEEN_CHECKPOINTS_MS"))
MAX_CONCURRENT_CHECKPOINTS=int(os.environ.get("MAX_CONCURRENT_CHECKPOINTS"))

"""
Configure checkpoints for stream
"""
def configure_checkpoints(
        env: StreamExecutionEnvironment
    ):
    config = env.get_checkpoint_config()
    if config is None:
        config = CheckpointConfig()

    # set mode to exactly-once (this is the default)
    config.set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)

    # make sure 500 ms of progress happen between checkpoints
    config.set_min_pause_between_checkpoints(MIN_PAUSE_BETWEEN_CHECKPOINTS_MS)

    # checkpoints have to complete within one minute, or are discarded
    config.set_checkpoint_timeout(CHECKPOINT_TIMEOUT_MS)

    # only two consecutive checkpoint failures are tolerated
    config.set_tolerable_checkpoint_failure_number(TOLERABLE_CHECKPOINT_FAILURE_NUMBER)

    # allow only one checkpoint to be in progress at the same time
    config.set_max_concurrent_checkpoints(MAX_CONCURRENT_CHECKPOINTS)

    # enable externalized checkpoints which are retained after job cancellation
    config.enable_externalized_checkpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    # enables the unaligned checkpoints
    config.enable_unaligned_checkpoints()
    
    storage = FileSystemCheckpointStorage(
        CHECKPOINT_DESTINATION
    )

    config.set_checkpoint_storage(storage)

    env.configure(config)

    # start a checkpoint every `CHECKPOINT_TIME_MS` ms
    env.enable_checkpointing(CHECKPOINT_TIME_MS)
