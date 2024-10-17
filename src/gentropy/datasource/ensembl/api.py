"""Methods that interact with Ensembl's API."""

from typing import Any


def fetch_coordinates_from_rsids(
    rsids: list[str], batch_size: int = 200, pause_time: int = 1
) -> dict[str, list[str]]:
    """Batch query the Ensembl API to extract variant coordinates from a list of rsIds.

    Args:
        rsids (list[str]): List of rsIDs
        batch_size (int): Number of rsIDs to process in each batch.
        pause_time (int): Time to pause between batches.

    Returns:
        dict[str, list[str]]: Dictionary with rsID as key and list of variant IDs as value.

    Raises:
        Exception: If an error occurs while processing the batches.

    Example:
        >>> fetch_coordinates_from_rsids(["rs75493593"])  # doctest: +SKIP
        {'rs75493593': ['17_7041768_G_C', '17_7041768_G_T']}
    """

    def _ensembl_batch_request(rsids: list[str]) -> dict[str, dict[str, Any]]:
        """Access the batch endpoint of Ensembl.

        Args:
            rsids (list[str]): List of rsIDs

        Returns:
            dict[str, dict[str, Any]]: Dictionary with rsID as key and variant data as value.
        """
        url = "https://rest.ensembl.org/variation/human"
        headers = {"Content-Type": "application/json", "Accept": "application/json"}
        data = json.dumps({"ids": rsids}).encode("utf-8")

        req = request.Request(url, data=data, headers=headers, method="POST")
        with request.urlopen(req) as response:
            response_data = response.read().decode()

        return json.loads(response_data)

    def _parse_response(
        response: dict[str, dict[str, Any]],
    ) -> dict[str, list[str]]:
        """Parse the response from the Ensembl API.

        Args:
            response (dict[str, dict[str, Any]]): Response from the Ensembl API. This is a dictionary where the key is the rsID and the value is the variant data.

        Returns:
            dict[str, list[str]]: Dictionary with rsID as key and list of variant IDs as value.
        """
        parsed_results = {}
        valid_chromosomes = [str(i) for i in range(1, 23)] + ["X", "Y", "MT"]
        for rsid in response:
            if response[rsid]["mappings"]:
                for data in response[rsid]["mappings"]:
                    if data["seq_region_name"] in valid_chromosomes:
                        chrom = data["seq_region_name"]
                        pos = data["start"]
                        # The first element of allele_string contains the reference allele and the rest are the alternate alleles
                        ref_allele = data["allele_string"].split("/")[0]
                        alt_alleles = data["allele_string"].split("/")[1:]
                        variant_ids = [
                            f"{chrom}_{pos}_{ref_allele}_{alt_allele}"
                            for alt_allele in alt_alleles
                        ]
                        parsed_results[rsid] = variant_ids
            else:
                continue
        return parsed_results

    import json
    import time
    from urllib import request

    all_results = {}

    # Chunk the rsids into batches
    for i in range(0, len(rsids), batch_size):
        batch = rsids[i : i + batch_size]
        try:
            variant_data = _ensembl_batch_request(batch)
            all_results.update(_parse_response(variant_data))

        except Exception as e:
            raise Exception(f"Error processing batch {i // batch_size + 1}: {e}") from e

        time.sleep(pause_time)

    return all_results
