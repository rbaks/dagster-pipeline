import json
import dagster as dg
from dagster_gcp.gcs import GCSResource

import io
import os
import base64
import hashlib
import tempfile

from pydub import AudioSegment

from dagster_tutorial.defs.extraction.assets import annotations

inner_id_partition = dg.DynamicPartitionsDefinition(name="inner_id_partition")


@dg.asset_sensor(asset_key=dg.AssetKey("annotations"))
def partition_creator_sensor(
    context: dg.SensorEvaluationContext, asset_event: dg.AssetMaterialization
):
    materialization = asset_event.dagster_event.event_specific_data.materialization

    # Try the convenient mapping first (returns MetadataValue wrappers)
    inner = (
        materialization.metadata.get("inner_ids") if materialization.metadata else None
    )

    # Fallback: older structure - look through metadata_entries
    if inner is None:
        for entry in getattr(materialization, "metadata_entries", []):
            if getattr(entry, "label", None) == "inner_ids":
                entry_data = getattr(entry, "entry_data", None)
                # entry_data may expose .value (JsonMetadataValue) or .json_str
                if entry_data is None:
                    inner = None
                elif hasattr(entry_data, "value"):
                    inner = entry_data.value
                elif hasattr(entry_data, "json_str"):
                    try:
                        inner = json.loads(entry_data.json_str)
                    except Exception:
                        inner = entry_data.json_str
                break

    # Unwrap MetadataValue-like objects which expose `.value`
    if inner is not None and hasattr(inner, "value"):
        inner = inner.value

    # Normalize into a list of strings (nothing to add if no keys)
    if not inner:
        return

    if not isinstance(inner, (list, tuple, set)):
        partition_keys = [inner]
    else:
        partition_keys = list(inner)

    partition_keys = [str(k) for k in partition_keys]

    context.instance.add_dynamic_partitions(
        partitions_def_name="inner_id_partition",
        partition_keys=partition_keys,
    )


@dg.asset(kinds={"gcs"}, partitions_def=inner_id_partition, code_version="v1")
def audio_slices(context: dg.AssetExecutionContext, annotations, gcs: GCSResource):
    from .utils import slice_audio_segments_bytes

    inner_id = context.partition_key

    annotations_same_inner_id = [
        item for item in annotations if str(item.get("inner_id")) == inner_id
    ]

    return slice_audio_segments_bytes(
        gcs.get_client(),
        annotations_same_inner_id,
    )


@dg.asset(kinds={"json"}, partitions_def=inner_id_partition, code_version="v1")
def augmented_slices(context: dg.AssetExecutionContext, audio_slices):
    from .utils import augment_audio_slices

    partition_key = context.partition_key

    audio_slices_same_inner_id = [
        item for item in audio_slices if str(item.get("inner_id")) == partition_key
    ]

    return augment_audio_slices(audio_slices_same_inner_id)
