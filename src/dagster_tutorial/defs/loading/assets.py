import io
import soundfile as sf
from datasets import Dataset, Features, Audio, Value
import dagster as dg

from dagster_tutorial.defs.transformation.assets import augmented_slices

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
