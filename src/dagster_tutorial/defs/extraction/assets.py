import json
import dagster as dg

from dagster_tutorial.defs.resources import LabelStudioRessource
from .utils import process_project_json


@dg.asset(kinds={"json", "source"}, code_version="v1")
def label_studio_project_json(label_studio: LabelStudioRessource):
    return label_studio.export_projects()


@dg.asset(kinds={"python"}, code_version="v1")
def annotations(context: dg.AssetExecutionContext, label_studio_project_json):
    results = process_project_json(label_studio_project_json)
    inner_ids = list(set([item["inner_id"] for item in results]))
    context.add_asset_metadata({"inner_ids": dg.MetadataValue.json(inner_ids)})
    return results
