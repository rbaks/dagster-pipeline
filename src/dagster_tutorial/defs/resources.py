import requests

import dagster as dg
from dagster_gcp.gcs import GCSPickleIOManager, GCSResource


class LabelStudioRessource(dg.ConfigurableResource):
    host_url: str
    legacy_token: str
    project_id: str

    @property
    def headers(self):
        return {"Authorization": f"Token  {self.legacy_token}"}

    @property
    def query_string(self):
        return f"{self.host_url}/api/projects/{self.project_id}/export?download_all_tasks=true"

    def export_projects(self):
        data = requests.get(self.query_string, headers=self.headers)
        data.raise_for_status()
        return data.json()


@dg.definitions
def resources():
    gcs_resource = GCSResource()
    return dg.Definitions(
        resources={
            "label_studio": LabelStudioRessource(
                host_url=dg.EnvVar("LABEL_STUDIO_HOST_URL"),
                legacy_token=dg.EnvVar("LABEL_STUDIO_LEGACY_TOKEN"),
                project_id=dg.EnvVar("LABEL_STUDIO_PROJECT_ID"),
            ),
            "gcs": gcs_resource,
            "io_manager": GCSPickleIOManager(
                gcs_bucket=dg.EnvVar("GCS_BUCKET_NAME"),
                gcs_prefix=dg.EnvVar("GCS_AUDIO_FOLDER_NAME"),
                gcs=gcs_resource,
            ),
        }
    )
