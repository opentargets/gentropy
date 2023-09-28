"""Google Cloud Storage Connector."""
# Copyright (c) 2020 bw2
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
from __future__ import annotations

import argparse
import glob
import logging
import os
import urllib.request
import xml.etree.ElementTree as ET

from pyspark.find_spark_home import _find_spark_home

logging.basicConfig(
    format="%(asctime)s %(levelname)-8s %(message)s", level=logging.INFO
)


def parse_connector_version(version):
    """Parse a connector version string into a tuple (hadoop version, major version, minor version, patch version, release candidate)."""
    hadoop_version, version = version.split("-", maxsplit=1)
    hadoop_version = int(hadoop_version[6:])

    release_candidate = None
    if "-" in version:
        version, tag = version.split("-", maxsplit=1)
        if tag.startswith("RC"):
            release_candidate = int(tag[2:])

    major_version, minor_version, patch_version = map(int, version.split("."))

    return (
        hadoop_version,
        major_version,
        minor_version,
        patch_version,
        release_candidate or float("inf"),
    )


def get_gcs_connector_url():
    """Get the URL of the jar file for the latest version of the Hadoop 2 connector."""
    with urllib.request.urlopen(
        "https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/maven-metadata.xml"
    ) as f:
        metadata = f.read().decode("utf-8")

    versions = [
        el.text
        for el in ET.fromstring(metadata).findall("./versioning/versions/version")
    ]
    hadoop2_versions = [
        version for version in versions if version.startswith("hadoop2-")
    ]
    latest_version = sorted(hadoop2_versions, key=parse_connector_version)[-1]

    return f"https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/{latest_version}/gcs-connector-{latest_version}-shaded.jar"


def parse_args():
    """Parse arguments."""

    def key_file_sort(file_path):
        """Sorts files by creation time in reverse order.

        Args:
          file_path: A string representing the path to the file.

        Returns:
          A negative integer representing the creation time of the file.
        """
        return -1 * os.path.getctime(file_path)

    def find_key_file(key_file_regexps):
        """Searches for a key file in the given locations.

        Args:
          key_file_regexps: A list of regular expressions representing the locations to search for the key file.

        Returns:
          The path to the key file, or None if no key file is found.
        """
        for key_file_regexp in key_file_regexps:
            paths = sorted(
                glob.glob(os.path.expanduser(key_file_regexp)), key=key_file_sort
            )
            if paths:
                return next(iter(paths))

        return None

    p = argparse.ArgumentParser()
    p.add_argument(
        "-k",
        "--key-file-path",
        help="Service account key .json path. This path is just added to the spark config file. The .json file itself doesn't need to exist until the GCS connector is first used.",
    )
    p.add_argument(
        "--gcs-requester-pays-project",
        "--gcs-requestor-pays-project",
        help="If specified, this google cloud project will be charged for access to "
        "requester pays buckets via spark/hadoop. See https://github.com/GoogleCloudDataproc/hadoop-connectors/blob/master/gcs/CONFIGURATION.md#cloud-storage-requester-pays-feature-configuration",
    )

    args = p.parse_args()

    if args.key_file_path and not os.path.isfile(args.key_file_path):
        logging.warning(f"{args.key_file_path} file doesn't exist")

    if not args.key_file_path:
        # look for existing key files in ~/.config
        key_file_regexps = [
            "~/.config/gcloud/application_default_credentials.json",
            "~/.config/gcloud/legacy_credentials/*/adc.json",
        ]

        # if more than one file matches a glob pattern, select the newest.
        args.key_file_path = find_key_file(key_file_regexps)
        if args.key_file_path:
            logging.info(f"Using key file: {args.key_file_path}")
        else:
            regexps_string = "    ".join(key_file_regexps)
            p.error(
                f"No json key files found in these locations: \n\n    {regexps_string}\n\n"
                "Run \n\n  gcloud auth application-default login \n\nthen rerun this script, "
                "or use --key-file-path to specify where the key file exists (or will exist later).\n"
            )

    return args


def check_dataproc_VM():
    """Check if this installation is running on a Dataproc VM."""
    try:
        response = urllib.request.urlopen(
            "http://metadata.google.internal/0.1/meta-data/attributes/dataproc-bucket"
        )
    except urllib.error.URLError:
        return False

    if response.status == 200 and response.getheader("Content-Type") == "text/plain":
        data = response.read().decode("UTF-8")
        if data.startswith("dataproc"):
            return True

    return False


def main():
    """Run main."""
    if check_dataproc_VM():
        logging.info(
            "This is a Dataproc VM which should already have the GCS cloud connector installed. Exiting..."
        )
        return

    args = parse_args()

    spark_home = _find_spark_home()

    # download GCS connector jar
    try:
        gcs_connector_url = get_gcs_connector_url()
    except Exception as e:
        logging.error(f"get_gcs_connector_url() failed: {e}")
        return

    local_jar_path = os.path.join(
        spark_home, "jars", os.path.basename(gcs_connector_url)
    )
    logging.info(f"Downloading {gcs_connector_url}")
    logging.info(f"   to {local_jar_path}")

    try:
        urllib.request.urlretrieve(gcs_connector_url, local_jar_path)
    except Exception as e:
        logging.error(f"Unable to download GCS connector to {local_jar_path}. {e}")
        return

    # update spark-defaults.conf
    spark_config_dir = os.path.join(spark_home, "conf")
    if not os.path.exists(spark_config_dir):
        os.mkdir(spark_config_dir)
    spark_config_file_path = os.path.join(spark_config_dir, "spark-defaults.conf")
    logging.info(f"Updating {spark_config_file_path} json.keyfile")
    logging.info(f"Setting json.keyfile = {args.key_file_path}")

    spark_config_lines = [
        "spark.hadoop.google.cloud.auth.service.account.enable true\n",
        f"spark.hadoop.google.cloud.auth.service.account.json.keyfile {args.key_file_path}\n",
    ]

    if args.gcs_requester_pays_project:
        spark_config_lines.extend(
            [
                "spark.hadoop.fs.gs.requester.pays.mode AUTO\n",
                f"spark.hadoop.fs.gs.requester.pays.project.id {args.gcs_requester_pays_project}\n",
            ]
        )

    try:
        # spark hadoop options docs @ https://github.com/GoogleCloudDataproc/hadoop-connectors/blob/master/gcs/CONFIGURATION.md#cloud-storage-requester-pays-feature-configuration
        if os.path.isfile(spark_config_file_path):
            with open(spark_config_file_path, "rt") as f:
                for line in f:
                    # avoid duplicating options
                    if any(
                        [option.split(" ")[0] in line for option in spark_config_lines]
                    ):
                        continue

                    spark_config_lines.append(line)

        with open(spark_config_file_path, "wt") as f:
            for line in spark_config_lines:
                f.write(line)

    except Exception as e:
        logging.error(f"Unable to update spark config {spark_config_file_path}. {e}")
        return


if __name__ == "__main__":
    main()
