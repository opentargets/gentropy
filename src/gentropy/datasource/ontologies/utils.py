"""Utility functions for Biosample ontology processing."""
import owlready2
from pyspark.sql import Row, SparkSession
from pyspark.sql.types import StructType, StringType, ArrayType
from pyspark.sql.functions import col, explode_outer, collect_set, collect_list, array_distinct, regexp_replace, udf, coalesce
from pyspark.sql.window import Window
from functools import reduce
from gentropy.dataset.biosample_index import BiosampleIndex


def extract_ontology_info(
    ontology : owlready2.namespace.Ontology,
    spark : SparkSession,
    schema : StructType
) -> BiosampleIndex:
    """Extracts the ontology information from Uberon or Cell Ontology owo owlready2 ontology object.
    NOT IN USE

    Args:
        ontology (owlready2.namespace.Ontology): An owlready2 ontology object. Must be either from Cell Ontology or Uberon.
        prefix (str): Prefix for the desired ontology terms.
        session (Session): Spark session.

    Returns:
        BiosampleIndex: Parsed and annotated biosample index table.
    """
    data_list = []

    # Iterate over all classes in the ontology
    for cls in ontology.classes():
        # Basic class information
        cls_id = cls.name
        # cls_code = cls.iri
        cls_name = cls.label[0] if cls.label else None

        # Extract descriptions
        description = None
        if hasattr(cls, 'IAO_0000115'):
            description = cls.IAO_0000115.first() if cls.IAO_0000115 else None

        # Extract dbXRefs
        dbXRefs = []
        if hasattr(cls, 'hasDbXref'):
            dbXRefs = [Row(id=x, source=x.split(':')[0]) for x in cls.hasDbXref]

        # Parent classes
        parents = []
        for parent in cls.is_a:
            if parent is owl.Thing: 
                continue  # Skip owlready2 Thing class, which is a top-level class
            elif hasattr(parent, 'name'):
                parent_id = parent.name
                parents.append(parent_id)
            elif hasattr(parent, 'property'):  # For restrictions
                continue  # We skip restrictions in this simplified list

        # Synonyms
        synonyms = set()
        if hasattr(cls, 'hasExactSynonym'):
            synonyms.update(cls.hasExactSynonym)
        if hasattr(cls, 'hasBroadSynonym'):
            synonyms.update(cls.hasBroadSynonym)
        if hasattr(cls, 'hasNarrowSynonym'):
            synonyms.update(cls.hasNarrowSynonym)
        if hasattr(cls, 'hasRelatedSynonym'):
            synonyms.update(cls.hasRelatedSynonym)

        # Children classes
        children = [child.name for child in cls.subclasses()]

        # Ancestors and descendants with Thing class filtered out
        ancestors = [anc.name for anc in cls.ancestors() if hasattr(anc, 'name') and anc is not owl.Thing]
        descendants = [desc.name for desc in cls.descendants() if hasattr(desc, 'name')]

        # Check if the class is deprecated
        is_deprecated = False
        if hasattr(cls, 'deprecated') and cls.deprecated:
            is_deprecated = True

        # Compile all information into a Row
        entry = Row(
            id=cls_id,
            # code=cls_code,
            name=cls_name,  
            dbXRefs=dbXRefs,
            description=description,
            parents=parents,
            synonyms=list(synonyms),
            ancestors=ancestors,
            descendants=descendants,
            children=children,
            ontology={"is_obsolete": is_deprecated}
        )
        
        # Add to data list
        data_list.append(entry)


    # Create DataFrame directly from Rows
    df = spark.createDataFrame(data_list, schema)
    return df


def extract_ontology_from_json(
    ontology_json : str,
    spark : SparkSession
) -> BiosampleIndex:
    """
    Extracts the ontology information from a JSON file. Currently only supports Uberon and Cell Ontology.

    Args:
        ontology_json (str): Path to the JSON file containing the ontology information.
        spark (SparkSession): Spark session.

    Returns:
        BiosampleIndex: Parsed and annotated biosample index table.
    """

    def json_graph_traversal(df, node_col, link_col, traversal_type="ancestors"):
        """
        Traverse a graph represented in a DataFrame to find all ancestors or descendants.
        """
        # Collect graph data as a map
        graph_map = df.select(node_col, link_col).rdd.collectAsMap()
        broadcasted_graph = spark.sparkContext.broadcast(graph_map)

        def get_relationships(node):
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
        relationship_udf = udf(get_relationships, ArrayType(StringType()))

        # Apply the UDF to create the result column
        return df.withColumn(result_col, relationship_udf(col(node_col)))

    # Load the JSON file
    df = spark.read.json(ontology_json, multiLine=True)

    # Exploding the 'graphs' array to make individual records easier to access
    df_graphs = df.select(explode_outer("graphs").alias("graph"))

    # Exploding the 'nodes' array within each graph
    df_nodes = df_graphs.select(
        col("graph.id").alias("graph_id"),
        explode_outer("graph.nodes").alias("node"))

    # Exploding the 'edges' array within each graph for relationship data
    df_edges = df_graphs.select(
        col("graph.id").alias("graph_id"),
        explode_outer("graph.edges").alias("edge")
    ).select(
        col("edge.sub").alias("subject"),
        col("edge.pred").alias("predicate"),
        col("edge.obj").alias("object")
    )
    df_edges = df_edges.withColumn("subject", regexp_replace(col("subject"), "http://purl.obolibrary.org/obo/", ""))
    df_edges = df_edges.withColumn("object", regexp_replace(col("object"), "http://purl.obolibrary.org/obo/", ""))

    # Extract the relevant information from the nodes
    transformed_df = df_nodes.select(
    regexp_replace(col("node.id"), "http://purl.obolibrary.org/obo/", "").alias("biosampleId"),
    col("node.lbl").alias("biosampleName"),
    col("node.meta.definition.val").alias("description"),
    collect_set(col("node.meta.xrefs.val")).over(Window.partitionBy("node.id")).getItem(0).alias("dbXrefs"),
    collect_set(col("node.meta.synonyms.val")).over(Window.partitionBy("node.id")).getItem(0).alias("synonyms"),
    col("node.meta.deprecated").alias("deprecated"))
    
    # Extract the relationships from the edges
    # Prepare relationship-specific DataFrames
    df_parents = df_edges.filter(col("predicate") == "is_a").select("subject", "object").withColumnRenamed("object", "parent")
    df_children = df_edges.filter(col("predicate") == "is_a").select("object", "subject").withColumnRenamed("subject", "child")

    # Aggregate relationships back to nodes
    df_parents_grouped = df_parents.groupBy("subject").agg(array_distinct(collect_list("parent")).alias("parents"))
    df_children_grouped = df_children.groupBy("object").agg(array_distinct(collect_list("child")).alias("children"))

    # Get all ancestors
    df_with_ancestors = json_graph_traversal(df_parents_grouped, "subject", "parents", "ancestors")
    # Get all descendants
    df_with_descendants = json_graph_traversal(df_children_grouped, "object", "children", "descendants")

    # Join the ancestor and descendant DataFrames
    df_with_relationships = df_with_ancestors.join(df_with_descendants, df_with_ancestors.subject == df_with_descendants.object, "full_outer").withColumn("biosampleId", coalesce(df_with_ancestors.subject, df_with_descendants.object)).drop("subject", "object")

    # Join the original DataFrame with the relationship DataFrame
    final_df = transformed_df.join(df_with_relationships, ['biosampleId'], "left")
    
    return final_df

    def merge_biosample_indices(
         biosample_indices: list[BiosampleIndex], 
    ) -> BiosampleIndex:
        """Merge a list of biosample indexes into a single biosample index.
        Where there are conflicts, in single values - the first value is taken. In list values, the union of all values is taken.

        Args:
            biosample_indexes (BiosampleIndex): Biosample indexes to merge.

        Returns:
            BiosampleIndex: Merged biosample index.
        """
        # Merge the DataFrames
        merged_df = reduce(DataFrame.unionByName, biosample_indices)