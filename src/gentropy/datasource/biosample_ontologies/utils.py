"""Utility functions for Biosample ontology processing."""

import re

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.window import Window

from gentropy.dataset.biosample_index import BiosampleIndex


def extract_ontology_from_json(
    ontology_json: str, spark: SparkSession
) -> BiosampleIndex:
    """Extracts the ontology information from a JSON file. Currently only supports Uberon and Cell Ontology.

    Args:
        ontology_json (str): Path to the JSON file containing the ontology information.
        spark (SparkSession): Spark session.

    Returns:
        BiosampleIndex: Parsed and annotated biosample index table.
    """

    def json_graph_traversal(
        df: DataFrame, node_col: str, link_col: str, traversal_type: str
    ) -> DataFrame:
        """Traverse a graph represented in a DataFrame to find all ancestors or descendants.

        Args:
            df (DataFrame): DataFrame containing the graph data.
            node_col (str): Column name for the node.
            link_col (str): Column name for the link.
            traversal_type (str): Type of traversal - "ancestors" or "descendants".

        Returns:
            DataFrame: DataFrame with the result column added.
        """
        # Collect graph data as a map
        graph_map = df.select(node_col, link_col).rdd.collectAsMap()
        broadcasted_graph = spark.sparkContext.broadcast(graph_map)

        def get_relationships(node: str) -> list[str]:
            """Get all relationships for a given node.

            Args:
                node (str): Node ID.

            Returns:
                list[str]: List of relationships.
            """
            relationships = set()
            stack = [node]
            while stack:
                current = stack.pop()
                if current in broadcasted_graph.value:
                    current_links = broadcasted_graph.value[current]
                    stack.extend(current_links)
                    relationships.update(current_links)
            return list(relationships)

        # Choose column name based on traversal type
        result_col = "ancestors" if traversal_type == "ancestors" else "descendants"

        # Register the UDF based on traversal type
        relationship_udf = f.udf(get_relationships, ArrayType(StringType()))

        # Apply the UDF to create the result column
        return df.withColumn(result_col, relationship_udf(f.col(node_col)))

    # Load the JSON file
    df = spark.read.json(ontology_json, multiLine=True)

    # Exploding the 'graphs' array to make individual records easier to access
    df_graphs = df.select(f.explode_outer("graphs").alias("graph"))

    # Exploding the 'nodes' array within each graph
    df_nodes = df_graphs.select(
        f.col("graph.id").alias("graph_id"),
        f.explode_outer("graph.nodes").alias("node"),
    )

    # Exploding the 'edges' array within each graph for relationship data
    df_edges = df_graphs.select(
        f.col("graph.id").alias("graph_id"),
        f.explode_outer("graph.edges").alias("edge"),
    ).select(
        f.col("edge.sub").alias("subject"),
        f.col("edge.pred").alias("predicate"),
        f.col("edge.obj").alias("object"),
    )

    # Remove certain URL prefixes from IDs
    urls_to_remove = ["http://purl.obolibrary.org/obo/", "http://www.ebi.ac.uk/efo/"]
    # Create a regex pattern that matches any of the URLs
    escaped_urls_pattern = "|".join([re.escape(url) for url in urls_to_remove])

    df_edges = df_edges.withColumn(
        "subject", f.regexp_replace(f.col("subject"), escaped_urls_pattern, "")
    )
    df_edges = df_edges.withColumn(
        "object", f.regexp_replace(f.col("object"), escaped_urls_pattern, "")
    )

    # Extract the relevant information from the nodes
    transformed_df = df_nodes.select(
        f.regexp_replace(f.col("node.id"), escaped_urls_pattern, "").alias(
            "biosampleId"
        ),
        f.coalesce(f.col("node.lbl"), f.col("node.id")).alias("biosampleName"),
        f.col("node.meta.definition.val").alias("description"),
        f.collect_set(f.col("node.meta.xrefs.val"))
        .over(Window.partitionBy("node.id"))
        .getItem(0)
        .alias("xrefs"),
        f.collect_set(f.col("node.meta.synonyms.val"))
        .over(Window.partitionBy("node.id"))
        .getItem(0)
        .alias("synonyms"),
    )

    # Extract the relationships from the edges
    # Prepare relationship-specific DataFrames
    df_parents = (
        df_edges.filter(f.col("predicate") == "is_a")
        .select("subject", "object")
        .withColumnRenamed("object", "parent")
    )
    df_children = (
        df_edges.filter(f.col("predicate") == "is_a")
        .select("object", "subject")
        .withColumnRenamed("subject", "child")
    )

    # Aggregate relationships back to nodes
    df_parents_grouped = df_parents.groupBy("subject").agg(
        f.array_distinct(f.collect_list("parent")).alias("parents")
    )
    df_children_grouped = df_children.groupBy("object").agg(
        f.array_distinct(f.collect_list("child")).alias("children")
    )

    # Get all ancestors
    df_with_ancestors = json_graph_traversal(
        df_parents_grouped, "subject", "parents", "ancestors"
    )
    # Get all descendants
    df_with_descendants = json_graph_traversal(
        df_children_grouped, "object", "children", "descendants"
    )

    # Join the ancestor and descendant DataFrames
    df_with_relationships = (
        df_with_ancestors.join(
            df_with_descendants,
            df_with_ancestors.subject == df_with_descendants.object,
            "full_outer",
        )
        .withColumn(
            "biosampleId",
            f.coalesce(df_with_ancestors.subject, df_with_descendants.object),
        )
        .drop("subject", "object")
    )

    # Join the original DataFrame with the relationship DataFrame
    final_df = transformed_df.join(df_with_relationships, ["biosampleId"], "left")

    return BiosampleIndex(_df=final_df, _schema=BiosampleIndex.get_schema())
