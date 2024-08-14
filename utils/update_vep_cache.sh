#!/user/bin/env bash
VEP_CACHE_BUCKET=gs://genetics_etl_python_playground/vep/cache


logging(){
    log_prompt="[$(date "+%Y.%m.%d %H:%M")]"
    echo "${log_prompt} $@" >> ${LOG_FILE}
}

# Fail if bgzip is not installe:
if [[ ! -x $(command -v bgzip) ]]; then
    logging "bgzip is not installed. See documentation for installation: https://github.com/DataBiosphere/bgzip/blob/master/README.md"
    logging "Exiting."
    exit 1
fi

# Test if jq is installed:
if [[ ! -x $(command -v jq) ]]; then
    logging "jq is not installed. Installing jq."
    sudo apt-get install jq
fi

# Get most recent Ensembl release:
ENSEMBL_RELEASE=$(curl -s "https://rest.ensembl.org/info/data/?content-type=application/json" | jq '.releases[0]')

# Fetch the VEP cache files for the release:
curl -O https://ftp.ensembl.org/pub/release-${ENSEMBL_RELEASE}/fasta/homo_sapiens/dna/Homo_sapiens.GRCh38.dna.primary_assembly.fa.gz
gzip -d Homo_sapiens.GRCh38.dna.primary_assembly.fa.gz
bgzip Homo_sapiens.GRCh38.dna.primary_assembly.fa
logging "Copy fasta file to bucket..."
gsutil cp Homo_sapiens.GRCh38.dna.primary_assembly.fa.gz ${VEP_CACHE_BUCKET}

#
cd $HOME/.vep
curl -O https://ftp.ensembl.org/pub/release-112/variation/indexed_vep_cache/homo_sapiens_vep_112_GRCh38.tar.gz
tar xzf homo_sapiens_vep_112_GRCh38.tar.gz


# Pulling plugins from github and update the tss plugin:
git clone https://github.com/Ensembl/VEP_plugins

# Patching the TSS distance pluging to make sure the distance is always provided:
echo -e """
package TSSDistance;\nuse Bio::EnsEMBL::Variation::Utils::BaseVepPlugin;\nuse base qw(Bio::EnsEMBL::Variation::Utils::BaseVepPlugin);\n
sub get_header_info {return {TSSDistance => \"Distance from the transcription start site\"};}\n
sub feature_types {return ['Transcript'];}\n
sub variant_feature_types {return ['BaseVariationFeature'];}\n
sub run {\n\tmy (\$self, \$tva) = @_;\n\tmy \$t = \$tva->transcript;\n\tmy \$vf = \$tva->base_variation_feature;\n\tmy \$dist;\n\t\$dist = \$t->strand == 1 ? \$t->start - \$vf->end : \$vf->start - \$t->end;\n\treturn {TSSDistance => abs(\$dist),}\n};\n1;
""" > VEP_plugins/TSSDistance.pm
