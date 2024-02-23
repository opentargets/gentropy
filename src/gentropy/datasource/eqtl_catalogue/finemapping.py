from gentropy.common.session import Session
import pandas as pd
from pyspark.sql import Column, Window
import pyspark.sql.functions as f
from gentropy.common.spark_helpers import get_top_ranked_in_window
from gentropy.common.utils import parse_pvalue
from gentropy.dataset.study_locus import StudyLocus
from gentropy.dataset.study_index import StudyIndex

def extract_credible_set_index(cs_id: Column) -> Column:
    """Extract the credible set index from the cs_id."""
    return f.split(cs_id, "_L")[1].cast("int")

def extract_dataset_id_from_file_path(file_path: Column) -> Column:
    """Extract the dataset_id from the file_path. The dataset_id follows the pattern QTD{6}.
    
    Args:
        file_path: The file path.
    Returns:
        The dataset_id.
    Examples:
        >>> extract_dataset_id_from_file_path(f.lit("QTD000046.credible_sets.tsv")).show()
        +----------+
        |dataset_id|
        +----------+
        | QTD000046|
        +----------+
    """
    return f.regexp_extract(file_path, r"QTD\d{6}", 0).alias("dataset_id")


session = Session("yarn")
pd.DataFrame.iteritems = pd.DataFrame.items

sample_study_id = "QTS000004"
sample_dataset_id = "QTD000046"
folder_path = "gs://eqtl_catalog_data/tmp_susie_decompressed"

credible_sets_path = f"{folder_path}/{sample_dataset_id}.credible_sets.tsv"
lbf_path = f"{folder_path}/{sample_dataset_id}.lbf_variable.txt"
studies_metadata_path = "https://raw.githubusercontent.com/eQTL-Catalogue/eQTL-Catalogue-resources/master/data_tables/dataset_metadata.tsv"

credible_sets = (
    session.spark.read.csv(credible_sets_path, sep="\t", header=True)
    .persist()
)

#  molecular_trait_id | ENSG00000233359          
#  gene_id            | ENSG00000233359          
#  cs_id              | ENSG00000233359_L1       
#  variant            | chr1_102291687_A_T       
#  rsid               | rs12044188               
#  cs_size            | 76                       
#  pip                | 0.0133246034625587       
#  pvalue             | 2.36065e-12              
#  beta               | -0.845301                
#  se                 | 0.0917599                
#  z                  | -10.1635388928995        
#  cs_min_r2          | 0.847176700121545        
#  region             | chr1:101389630-103389630 
#  credibleSetIndex   | 1                        
#  dataset_id         | QTD000046

lbf = session.spark.read.csv(lbf_path, sep="\t", header=True).persist()

#  molecular_trait_id | ENSG00000272279     
#  region             | chr6:528911-2528911 
#  variant            | chr6_529104_C_T     
#  chromosome         | 6                   
#  position           | 529104              
#  lbf_variable1      | -0.787007730605098  
#  lbf_variable2      | -0.245875563068143  
#  lbf_variable3      | -0.243956583413853  
#  lbf_variable4      | -0.246931930361598  
#  lbf_variable5      | -0.253445111259816  
#  lbf_variable6      | -0.261234926719349  
#  lbf_variable7      | -0.267987449936489  
#  lbf_variable8      | -0.271955807958227  
#  lbf_variable9      | -0.272196182176536  
#  lbf_variable10     | -0.268490278993762

studies_metadata = (
    session.spark.createDataFrame(pd.read_csv(studies_metadata_path, sep="\t"))
)

#  study_id        | QTS000001        
#  dataset_id      | QTD000001        
#  study_label     | Alasoo_2018      
#  sample_group    | macrophage_naive 
#  tissue_id       | CL_0000235       
#  tissue_label    | macrophage       
#  condition_label | naive            
#  sample_size     | 84               
#  quant_method    | ge

ss_ftp_path_template = "https://ftp.ebi.ac.uk/pub/databases/spot/eQTL/sumstats"
susie_results_df = (
    credible_sets
    .withColumn("dataset_id", extract_dataset_id_from_file_path(f.input_file_name()))
    # filter out credible sets from any method other than gene quantification
    .join(
        studies_metadata.filter(f.col("quant_method") == "ge"), on="dataset_id"
    )
    # bring in the lbf data
    .join(
        lbf,
        on = ["molecular_trait_id", "region", "variant"]
    )
    .withColumn(
        "logBF",
        f.when(f.col("credibleSetIndex") == 1, f.col("lbf_variable1"))
        .when(f.col("credibleSetIndex") == 2, f.col("lbf_variable2"))
        .when(f.col("credibleSetIndex") == 3, f.col("lbf_variable3"))
        .when(f.col("credibleSetIndex") == 4, f.col("lbf_variable4"))
        .when(f.col("credibleSetIndex") == 5, f.col("lbf_variable5"))
        .when(f.col("credibleSetIndex") == 6, f.col("lbf_variable6"))
        .when(f.col("credibleSetIndex") == 7, f.col("lbf_variable7"))
        .when(f.col("credibleSetIndex") == 8, f.col("lbf_variable8"))
        .when(f.col("credibleSetIndex") == 9, f.col("lbf_variable9"))
        .when(f.col("credibleSetIndex") == 10, f.col("lbf_variable10")),
    )
    .select(
        f.regexp_replace(f.col("variant"), r"chr", "").alias("variantId"),
        f.col("region"),
        f.col("chromosome"),
        f.col("position").cast("int"),
        f.col("pip").alias("posteriorProbability").cast("double"),
        *parse_pvalue(f.col('pvalue')),
        f.col("sample_size").cast("int").alias("nSamples"),
        f.col("beta").cast("double"),
        f.col("se").cast("double").alias("standardError"),
        extract_credible_set_index(f.col("cs_id")).alias("credibleSetIndex"),
        f.col("logBF").cast("double"),
        f.lit("SuSie").alias("finemappingMethod"),
        # Study metadata
        f.col("gene_id").alias("geneId"),
        f.array(f.col("molecular_trait_id")).alias("traitFromSourceMappedIds"),
        f.col("dataset_id"),
        f.concat_ws("_", f.col("study_label"), f.col("sample_group"), f.col("gene_id")).alias("studyId"),
        f.col("tissue_id").alias("tissueFromSourceId"),
        f.lit("eqtl").alias("studyType"),
        f.col("study_label").alias("projectId"),
        f.concat_ws("/", f.lit(ss_ftp_path_template), f.col("study_id"), f.col("dataset_id")).alias("summarystatsLocation"),
        f.lit(True).alias("hasSumstats"),
    )
    .withColumn(
        "studyLocusId",
        StudyLocus.assign_study_locus_id(f.col("studyId"), f.col("variantId"))
    )
    .persist()
)

susie_results_df.show(1, False, True)

#  variantId                | X_155720450_G_A                                                            
#  region                   | chrX:153886355-155886355                                                   
#  chromosome               | X                                                                          
#  position                 | 155720450                                                                  
#  posteriorProbability     | 0.00210960605295296                                                        
#  pValueMantissa           | 8.801                                                                      
#  pValueExponent           | -10                                                                        
#  sampleSize               | 65                                                                         
#  beta                     | -1.36377                                                                   
#  standardError            | 0.181018                                                                   
#  credibleSetIndex         | 1                                                                          
#  logBF                    | 19.0883379203313                                                           
#  finemappingMethod        | SuSie                                                                      
#  geneId                   | ENSG00000288722                                                            
#  traitFromSourceMappedIds | [ENSG00000288722]                                                          
#  dataset_id               | QTD000046                                                                  
#  studyId                  | Braineac2_substantia_nigra_ENSG00000288722                                 
#  tissueFromSourceId       | UBERON_0002038                                                             
#  study_type               | eqtl                                                                       
#  projectId                | Braineac2                                                                  
#  summarystatsLocation     | https://ftp.ebi.ac.uk/pub/databases/spot/eQTL/sumstats/QTS000004/QTD000046 
#  hasSumstats              | true                                                                       
#  studyLocusId             | -4685420340292829481 

### CREDIBLE SETS
lead_w = Window.partitionBy("studyId", "region", "credibleSetIndex")
study_locus_cols = [field.name for field in StudyLocus.get_schema().fields if field.name in susie_results_df.columns] + ["locus"]

harmonised_credible_sets = StudyLocus(
    _df=(
        susie_results_df
        .withColumn(
            "isLead", f.row_number().over(lead_w.orderBy(
                *[
                    f.col("pValueExponent").asc(),
                    f.col("pValueMantissa").asc(),
                ]
            )) == f.lit(1)
        )
        .withColumn(
            # Collecting all variants that constitute the credible set brings as many variants as the credible set size
            "locus",
            f.collect_set(
                f.struct(
                    "variantId",
                    "posteriorProbability",
                    "pValueMantissa",
                    "pValueExponent",
                    "logBF",
                    "beta",
                    "standardError",
                )
            ).over(lead_w)
        )
        .filter(f.col("isLead"))
        .select(study_locus_cols)
    ),
    _schema=StudyLocus.get_schema()
).annotate_credible_sets().persist()


## STUDY INDEX
study_index_cols = [field.name for field in StudyIndex.get_schema().fields if field.name in susie_results_df.columns]
harmonised_studies = StudyIndex(
    _df=susie_results_df.select(study_index_cols).distinct(),
    _schema=StudyIndex.get_schema()
).persist()
