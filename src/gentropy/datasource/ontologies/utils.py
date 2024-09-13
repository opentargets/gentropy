
def extract_ontology_info(
    ontology : owlready2.namespace.Ontology,
    session : Session,
    schema : StructType
) -> BiosampleIndex:
    """Extracts the ontology information from Uberon or Cell Ontology owo owlready2 ontology object.

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
    df = session.createDataFrame(data_list, schema)
    return df
