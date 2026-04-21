import io
import soundfile as sf
from datasets import Dataset, Features, Audio, Value
import dagster as dg

from dagster_tutorial.defs.transformation.assets import augmented_slices, audio_slices

from .utils import process_audio_item


@dg.asset(
    ins={"augmented_slices": dg.AssetIn(partition_mapping=dg.AllPartitionMapping())},
    code_version="v1",
)
def huggingface_dataset(context, augmented_slices):
    if not augmented_slices:
        context.log.info("No upstream partitions found. Returning empty dataset.")
        return Dataset.from_dict({})

    def example_generator():
        for partition_key in sorted(augmented_slices.keys()):
            partition_value = augmented_slices[partition_key]
            if not partition_value:
                continue

            for item in partition_value:
                if item.get("slice_number"):
                    processed_item = process_audio_item(item)

                    if processed_item is None:
                        continue

                    yield processed_item

    features = Features(
        {
            "audio": Audio(sampling_rate=None),  # None allows variable sample rates
            "text": Value("string"),
            "variant": Value("string"),
            "inner_id": Value("string"),
            "region_id": Value("string"),
            "region_number": Value("int32"),
            "is_agent": Value("bool"),
            "start": Value("float32"),
            "end": Value("float32"),
            "slice_number": Value("int32"),
            "sample_rate": Value("int32"),
        }
    )

    dataset = Dataset.from_generator(example_generator, features=features)
    dataset.push_to_hub(dg.EnvVar("HF_DATASET_PATH").get_value(), config_name="augmented")
    # return dataset


@dg.asset(
    ins={"audio_slices": dg.AssetIn(partition_mapping=dg.AllPartitionMapping())},
    code_version="v1",
)
def huggingface_dataset_slice_only(context, audio_slices):
    if not audio_slices:
        context.log.info("No upstream partitions found. Returning empty dataset.")
        return Dataset.from_dict({})

    def example_generator():
        for partition_key in sorted(audio_slices.keys()):
            partition_value = audio_slices[partition_key]
            if not partition_value:
                continue

            for item in partition_value:
                if item.get("slice_number"):
                    processed_item = process_audio_item(item)

                    if processed_item is None:
                        continue

                    yield processed_item

    features = Features(
        {
            "audio": Audio(sampling_rate=None),  # None allows variable sample rates
            "text": Value("string"),
            "variant": Value("string"),
            "inner_id": Value("string"),
            "region_id": Value("string"),
            "region_number": Value("int32"),
            "is_agent": Value("bool"),
            "start": Value("float32"),
            "end": Value("float32"),
            "slice_number": Value("int32"),
            "sample_rate": Value("int32"),
        }
    )

    dataset = Dataset.from_generator(example_generator, features=features)
    dataset.push_to_hub(dg.EnvVar("HF_DATASET_PATH").get_value())
    # return dataset


@dg.asset(
    ins={"audio_slices": dg.AssetIn(partition_mapping=dg.AllPartitionMapping())},
    code_version="v1",
)
def flush_slices(context, audio_slices):
    import json, os
    import tempfile
    from pydub import AudioSegment

    proof_list = []

    context.log.info(audio_slices)
    for idx, item in enumerate(audio_slices, start=1):
        slice_label = item.get("slice_number", idx)
        wav_bytes = item.get("sliced_audio_bytes")  # adjust key if yours is different

        if not wav_bytes:
            proof_list.append({"slice_number": slice_label, "status": "missing_bytes"})
            continue

        try:
            # Reconstruct AudioSegment
            bio = io.BytesIO(wav_bytes)
            segment = AudioSegment.from_file(bio, format="wav")

            # Metadata to prove reconstruction
            length_ms = len(segment)
            frame_rate = segment.frame_rate
            channels = segment.channels
            sample_width = segment.sample_width
            dbfs = segment.dBFS  # loudness measure
            bytes_len = len(wav_bytes)

            # Small numeric sample preview (first N samples) — safe and useful to inspect waveform content
            raw_samples = segment.get_array_of_samples()
            sample_preview = list(raw_samples[:10])  # first 10 sample values

            proof = {
                "slice_number": slice_label,
                "length_ms": length_ms,
                "frame_rate": frame_rate,
                "channels": channels,
                "sample_width": sample_width,
                "dBFS": dbfs,
                "bytes_len": bytes_len,
                "sample_preview": sample_preview,
            }

            proof_list.append(proof)

        except Exception as e:
            proof_list.append(
                {"slice_number": slice_label, "status": "error", "error": str(e)}
            )

    with open(
        "/teamspace/studios/this_studio/dagster-pipeline/src/dagster_tutorial/defs/data/output.json",
        "w",
        encoding="utf-8",
    ) as f:
        json.dumps(proof_list, f, indent=2, ensure_ascii=False)

    # return proof_list
