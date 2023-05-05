"""Generate jinja2 template for workflow."""
from __future__ import annotations

import yaml
from google.cloud import dataproc_v1 as dataproc
from google.cloud.dataproc_v1.types import (
    NodeInitializationAction,
    OrderedJob,
    WorkflowTemplate,
    WorkflowTemplatePlacement,
)
from google.protobuf.duration_pb2 import Duration

#
project_id = "open-targets-genetics-dev"
region = "europe-west1"
zone = "europe-west1-d"

# Managed cluster
python_cli = "gs://genetics_etl_python_playground/initialisation/cli.py"
config_name = "my_config"
config_tar = "gs://genetics_etl_python_playground/initialisation/config.tar.gz"
cluster_name = "ill-otg-cluster"
package_wheel = "gs://genetics_etl_python_playground/initialisation/otgenetics-0.1.4-py3-none-any.whl"
machine_type = "n1-highmem-96"
initialisation_executable_file = (
    "gs://genetics_etl_python_playground/initialisation/initialise_cluster.sh"
)
image_version = "2.1"
num_local_ssds = 0

# Available cluster
cluster_uuid = "eba42738-2ea3-4b0a-ba1d-38428427e838"

# job
python_cli = "gs://genetics_etl_python_playground/initialisation/cli.py"
cluster_config_dir = "/config"

# template
template_id = "do-ot-genetics-workflow"
dag_yaml = "workflow/dag.yaml"


def generate_available_placement_template(
    cluster_uuid: str,
) -> WorkflowTemplatePlacement:
    """Generates placement using available clusters.

    Args:
        cluster_uuid (str): Cluster UUID to use for placement.

    Returns:
        WorkflowTemplatePlacement: Placement template.
    """
    placement = dataproc.WorkflowTemplatePlacement()
    placement.cluster_selector.cluster_labels = {
        "goog-dataproc-cluster-uuid": cluster_uuid
    }
    return placement


def generate_managed_placement_template(
    cluster_name: str,
    config_tar: str,
    package_wheel: str,
    zone: str,
    machine_type: str,
    initialisation_executable_file: str,
    image_version: str,
    num_local_ssds: int = 0,
    initialisation_execution_timeout: str = "600s",
) -> WorkflowTemplatePlacement:
    """Generates placement using managed clusters.

    Args:
        cluster_name (str): Cluster name to use for placement.
        config_tar (str): Path to GS location with config tarball to use for cluster creation.
        package_wheel (str): Path to GS location with package wheel to use for cluster creation.
        zone (str): Zone to use for cluster creation.
        machine_type (str): Machine type to use for cluster creation.
        initialisation_executable_file (str): Path to GS location with initialisation script.
        image_version (str): Dataproc image version to use for cluster creation.
        num_local_ssds (int): Number of local SSDs to use for cluster creation. Defaults to 0.
        initialisation_execution_timeout (str): Initialisation script execution timeout. Defaults to "600s".

    Returns:
        WorkflowTemplatePlacement: Placement template.
    """
    placement = dataproc.WorkflowTemplatePlacement()
    placement.managed_cluster.cluster_name = cluster_name
    placement.managed_cluster.config.endpoint_config.enable_http_port_access = True
    placement.managed_cluster.config.gce_cluster_config.zone_uri = zone
    placement.managed_cluster.config.gce_cluster_config.metadata = {
        "CONFIGTAR": config_tar,
        "PACKAGE": package_wheel,
    }
    if num_local_ssds > 0:
        placement.managed_cluster.config.master_config.disk_config.num_local_ssds = (
            num_local_ssds
        )
    initialisation_node = NodeInitializationAction()
    initialisation_node.executable_file = initialisation_executable_file
    duration = Duration()
    duration.FromJsonString(initialisation_execution_timeout)
    initialisation_node.execution_timeout = duration
    placement.managed_cluster.config.initialization_actions = [initialisation_node]
    placement.managed_cluster.config.initialization_actions

    placement.managed_cluster.config.master_config.machine_type_uri = machine_type
    placement.managed_cluster.config.software_config.image_version = image_version
    placement.managed_cluster.config.software_config.properties = {
        "dataproc:dataproc.allow.zero.workers": "true"
    }
    return placement


def pyspark_job_template(
    step: dict,
    cluster_config_dir: str,
    config_name: str,
) -> OrderedJob:
    """Generates a pyspark job template.

    Args:
        step (dict): Step to generate job for.
        cluster_config_dir (str): Local path in the cluster where the config tarball is extracted.
        config_name (str): Name of the config file to use.

    Returns:
        OrderedJob: Pyspark job template.
    """
    job = OrderedJob()
    job.step_id = step["id"]
    job.pyspark_job.main_python_file_uri = python_cli
    job.pyspark_job.args = [
        f"step={ step['id'] }",
        f"--config-dir={ cluster_config_dir }",
        f"--config-name={ config_name }",
    ]
    # to provide hail support
    job.pyspark_job.properties = {
        "spark.jars": "/opt/conda/miniconda3/lib/python3.10/site-packages/hail/backend/hail-all-spark.jar",
        "spark.driver.extraClassPath": "/opt/conda/miniconda3/lib/python3.10/site-packages/hail/backend/hail-all-spark.jar",
        "spark.executor.extraClassPath": "./hail-all-spark.jar",
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "spark.kryo.registrator": "is.hail.kryo.HailKryoRegistrator",
    }
    # dependency steps
    if "prerequisites" in step:
        job.prerequisite_step_ids = step["prerequisites"]
    return job


def instantiate_inline_workflow_template(
    project_id: str, region: str, template: WorkflowTemplate
) -> None:
    """Submits a workflow for a Cloud Dataproc using the Python client library.

    Args:
        project_id (str): Project to use for running the workflow.
        region (str): Region where the workflow resources should live.
        template (WorkflowTemplate): Workflow template to submit.
    """
    # Create a client with the endpoint set to the desired region.
    workflow_template_client = dataproc.WorkflowTemplateServiceClient(
        client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"}
    )
    # Submit the request to instantiate the workflow from an inline template.
    operation = workflow_template_client.instantiate_inline_workflow_template(
        request={
            "parent": f"projects/{project_id}/regions/{region}",
            "template": template,
        }
    )
    operation.result()

    # Output a success message.
    print("Workflow ran successfully.")


def main() -> None:
    """Submit dataproc workflow."""
    template = dataproc.WorkflowTemplate()

    # Initialize request argument(s)
    template.id = template_id
    # template.placement = generate_available_placement_template(cluster_uuid)
    template.placement = generate_managed_placement_template(
        cluster_name,
        config_tar,
        package_wheel,
        zone,
        machine_type,
        initialisation_executable_file,
        image_version,
        num_local_ssds=num_local_ssds,
    )

    # Load steps from yaml file
    with open(dag_yaml, "r") as file:
        steps = yaml.safe_load(file)

    template.jobs = [
        pyspark_job_template(step, cluster_config_dir, config_name) for step in steps
    ]

    instantiate_inline_workflow_template(project_id, region, template)


if __name__ == "__main__":
    main()
