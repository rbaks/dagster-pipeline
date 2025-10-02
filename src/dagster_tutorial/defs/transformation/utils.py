import io
import random
import os
import dagster as dg

import numpy as np
import soundfile as sf
import torch

from pydub import AudioSegment
import tempfile
from urllib.parse import urlparse

from .audio_augmentation import compose_transform


def augment_audio_slices(audio_slices):
    results = []

    for s in audio_slices:
        wav_bytes = s.get("sliced_audio_bytes")
        if not wav_bytes:
            context.log.warning(f"slice {s.get('slice_number')} has no bytes, skipping")
            continue

        data, sr = sf.read(io.BytesIO(wav_bytes), dtype="float32")
        if data.ndim == 1:
            data = data[:, None]  # make (samples, 1)

        tensor = torch.from_numpy(data.T).float()

        augmented, final_sr = compose_transform((tensor, sr))

        if not isinstance(augmented, torch.Tensor):
            augmented = torch.tensor(augmented, dtype=torch.float32)

        augmented = augmented.detach().cpu().numpy().T  # (samples, channels)
        augmented = np.clip(augmented, -1.0, 1.0).astype("float32")

        out_buf = io.BytesIO()

        sf.write(out_buf, augmented, final_sr, format="WAV", subtype="PCM_16")
        augmented_bytes = out_buf.getvalue()

        original_bytes = wav_bytes

        meta = {
            "inner_id": s.get("inner_id"),
            "region_id": s.get("region_id"),
            "region_number": s.get("region_number"),
            "is_agent": s.get("is_agent"),
            "text": s.get("text"),
            "start": s.get("start"),
            "end": s.get("end"),
            "slice_number": s.get("slice_number"),
            "sample_rate": final_sr,  # Use final_sr for consistency
        }

        results.append({**meta, "variant": "original", "audio_bytes": original_bytes})
        results.append({**meta, "variant": "augmented", "audio_bytes": augmented_bytes})

    return results


def slice_audio_segments_bytes(client, processed_objects):
    if not processed_objects:
        return processed_objects

    inner_id = processed_objects[0]["inner_id"]
    audio_uri = processed_objects[0]["audio_uri"]

    for obj in processed_objects:
        if obj["inner_id"] != inner_id:
            raise ValueError(
                f"All objects must have the same inner_id. Expected {inner_id}, got {obj['inner_id']}"
            )
        if obj["audio_uri"] != audio_uri:
            raise ValueError(
                f"All objects with same inner_id must have the same audio_uri. Expected {audio_uri}, got {obj['audio_uri']}"
            )

    try:
        parsed_uri = urlparse(audio_uri)
        if parsed_uri.scheme != "gs":
            print(f"Warning: skipping non-GCS URI: {audio_uri}")
            for obj in processed_objects:
                obj["sliced_audio_bytes"] = None
            return processed_objects

        source_bucket_name = parsed_uri.netloc
        source_blob_name = parsed_uri.path.lstrip("/")

        source_bucket = client.bucket(source_bucket_name)
        source_blob = source_bucket.blob(source_blob_name)

        with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as temp_input:
            try:
                print(f"Downloading audio file: {audio_uri}")
                source_blob.download_to_filename(temp_input.name)

                print(f"Loading audio file for inner_id: {inner_id}")
                audio = AudioSegment.from_file(temp_input.name)

                # For each slice, export to BytesIO and attach bytes to object
                for slice_number, obj in enumerate(processed_objects, 1):
                    try:
                        start_ms = int(obj["start"] * 1000)
                        end_ms = int(obj["end"] * 1000)
                        sliced = audio[start_ms:end_ms]

                        buf = io.BytesIO()
                        # export WAV into buffer
                        sliced.export(buf, format="wav")
                        buf.seek(0)
                        obj["sliced_audio_bytes"] = buf.getvalue()

                        # optional: keep some metadata for downstream convenience
                        obj["slice_number"] = slice_number

                        print(
                            f"Prepared slice {slice_number} for inner_id {inner_id} (bytes={len(obj['sliced_audio_bytes'])})"
                        )

                    except Exception as e:
                        print(
                            f"Error processing slice {slice_number} for inner_id {inner_id}: {e}"
                        )
                        obj["sliced_audio_bytes"] = None

            finally:
                if os.path.exists(temp_input.name):
                    os.unlink(temp_input.name)

    except Exception as e:
        print(f"Error processing audio for inner_id {inner_id}: {e}")
        for obj in processed_objects:
            obj["sliced_audio_bytes"] = None

    return processed_objects
