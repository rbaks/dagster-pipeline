def process_project_json(json_data):
    all_processed_objects = []

    for task in json_data:
        audio_uri = task.get("data", {}).get("audio", "")
        inner_id = task.get("inner_id", None)
        task_id = task.get("id", None)

        for annotation in task.get("annotations", []):
            result_array = annotation.get("result", [])

            processed_objects = process_result_array(
                result_array, audio_uri, inner_id, task_id
            )
            all_processed_objects.extend(processed_objects)

    return all_processed_objects


def process_result_array(result_array, audio_uri, inner_id, task_id):
    grouped_by_id = {}

    for item in result_array:
        if "id" not in item or item["id"] is None:
            print(f"Warning: Skipping item without ID in task {task_id}: {item}")
            continue

        item_id = item["id"]
        if item_id not in grouped_by_id:
            grouped_by_id[item_id] = []
        grouped_by_id[item_id].append(item)

    processed_objects = []

    for idx, (item_id, items) in enumerate(grouped_by_id.items()):
        obj = {
            "inner_id": inner_id,
            "region_id": item_id,
            "region_number": idx + 1,
            "audio_uri": audio_uri,
            "start": None,
            "end": None,
            "is_agent": 0,
            "text": "",
        }

        for item in items:
            if "value" not in item:
                print(
                    f"Warning: Item {item_id} in task {task_id} missing 'value' field"
                )
                continue

            value = item["value"]

            if obj["start"] is None and "start" in value and "end" in value:
                obj["start"] = value["start"]
                obj["end"] = value["end"]

            if (
                item.get("type") == "labels"
                and "labels" in value
                and len(value["labels"]) > 0
            ):
                label = value["labels"][0]
                obj["is_agent"] = {"Agent": 1, "Client": 0}.get(label, 2)

            if (
                item.get("type") == "textarea"
                and "text" in value
                and isinstance(value["text"], list)
            ):
                obj["text"] = " ".join(value["text"])

        processed_objects.append(obj)

    processed_objects.sort(key=lambda x: x["start"] if x["start"] is not None else 0)

    return processed_objects
