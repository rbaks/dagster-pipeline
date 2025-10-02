import io
import dagster as dg
from datasets import Dataset, Audio, Features, Value
import soundfile as sf


def process_audio_item(item):
    audio_bytes = item.get("audio_bytes")

    if not audio_bytes:
        return None

    data, sr = sf.read(io.BytesIO(audio_bytes), dtype="float32")

    return {
        "audio": {"array": data, "sampling_rate": sr},
        "text": item.get("text", ""),
        "variant": item.get("variant", ""),
        "inner_id": item.get("inner_id", ""),
        "region_id": item.get("region_id", ""),
        "region_number": item.get("region_number", 0),
        "is_agent": item.get("is_agent", False),
        "start": item.get("start", 0.0),
        "end": item.get("end", 0.0),
        "slice_number": item.get("slice_number", 0),
        "sample_rate": sr,
    }
