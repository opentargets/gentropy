# -*- coding: utf-8 -*-
#
# Bruno Ariano
#
# Refactoring of CHEERS enrichment script using SPARK
#


import argparse
import hydra
from Utils import *
import os
import sys
from typing import Iterable
import time
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark import SparkConf
import pandas as pd
import numpy
import scipy.special as sps


def ndtr(x, mean, std):
    return .5 + .5*sps.erf((x - mean)/(std * 2**.5))

@hydra.main(config_path=os.getcwd(), config_name="config")
def main(cfg):

    start_time = time.time()


    # Make spark session
    global spark
    sparkConf = (
        SparkConf()
        .set("spark.hadoop.fs.gs.requester.pays.mode", "AUTO")
        .set("spark.hadoop.fs.gs.requester.pays.project.id", cfg.project.id)
        .set("spark.sql.broadcastTimeout", "36000")
    )

    # establish spark connection
    spark = SparkSession.builder.config(conf=sparkConf).master("yarn").getOrCreate()

    print('Spark version: ', spark.version)


    #
    # Load datasets -----------------------------------------------------------
    #


    peaks_wide = load_peaks(spark, cfg.files.peaks).persist()
    snps = load_snps(spark, cfg.files.snp)
    
    #
    # Melt the peaks dataset --------------------------------------------------
    #
    sample_names = peaks_wide.columns[3:]
    peaks_long = melt(
        df=peaks_wide,
        id_vars=['chr', 'start', 'end'],
        value_vars=sample_names,
        var_name='sample',
        value_name='score'
    )
    window_spec = (
        Window
        .partitionBy('sample')
        .orderBy('score')
    )

    # Get ranks. The original code starts ranks from 0, so subtract 1.
    peak_ranks = peaks_long.withColumn('rank', F.rank().over(window_spec) - 1)

    peaks_overlapping = (
        # Only need the peak coords
        peaks_wide
        .select('chr', 'start', 'end')
        # Do a inner join
        .join(
            snps,
            ((F.col('chrom') == F.col('chr')) &
             (F.col('pos') >= F.col('start')) &
             (F.col('pos') <= F.col('end'))
            ), how='inner')
        .drop('chrom')
        .select('study_id', 'chr', 'start', 'end')
        .distinct()
        # Persist as we need to count later
        .persist()
    )

    # Store total number of peaks and total overlapping peaks
    n_total_peaks = float(peaks_wide.count())
    n_unique_peaks = (peaks_overlapping
                        .groupBy('study_id')
                        .count()
                        .withColumnRenamed('count', 'count_peaks')
                    )
    
    # Get peaks that overlap
    unique_peaks = (peaks_overlapping
                    .join(peak_ranks, on = ['chr', 'start', 'end'], how = 'inner')
                    )
   

    # Get mean rank per sample
    sample_mean_rank = (
        unique_peaks
        .groupby('study_id','sample')
        .agg(
            F.mean(F.col('rank')).alias('mean_rank')
        )
        .cache()
    )

    # Define parameters for descrete uniform distribution and calculate p-value
    @F.pandas_udf('double')
    def mean_sd(x):
        return numpy.sqrt(((float(n_total_peaks)**2) - 1) / (12 * x))

    n_unique_peaks = n_unique_peaks.withColumn('mean_sd', mean_sd('count_peaks'))

    mean_mean = (1 + n_total_peaks) / 2
    sample_mean_rank_unique_peaks = sample_mean_rank.join(n_unique_peaks, on = 'study_id', how = 'inner').withColumn('mean_mean', F.lit(mean_mean))
    
    sample_mean_rank_unique_peaks = (sample_mean_rank_unique_peaks
                                    .withColumn("mean_mean",sample_mean_rank_unique_peaks.mean_mean.cast(FloatType()))
                                    .withColumn("mean_sd",sample_mean_rank_unique_peaks.mean_sd.cast(FloatType()))
                                    .withColumn("mean_rank",sample_mean_rank_unique_peaks.mean_rank.cast(FloatType()))
                                    )   
         
    @F.pandas_udf('double')
    def vectorized_cdf(x,y,z):
        return pd.Series(1- ndtr(x,z,y))

    sample_mean_rank_unique_peaks = (
        sample_mean_rank_unique_peaks
        .withColumn('pvalue', vectorized_cdf(sample_mean_rank_unique_peaks.mean_rank, sample_mean_rank_unique_peaks.mean_sd, sample_mean_rank_unique_peaks.mean_mean))
    )

    #
    # Write outputs -----------------------------------------------------------
    #

    # Write enrichment statsitics
    (
        sample_mean_rank_unique_peaks
        .select('study_id', 'sample', 'mean_rank', 'pvalue')
        .write.csv(
            cfg.files.out_stats,
            sep='\t'
        )
    )

    # Write a table of unique peaks
    if cfg.files.out_unique_peaks:
        (
            unique_peaks
            .write
            .csv(
                cfg.files.out_unique_peaks,
                sep='\t'
            )
        )

    # Write a table SNPs and their overlapping peaks
    if cfg.files.out_snp_peak_overlaps:
        
        # Write
        (
            peaks_overlapping
            .write
            .csv(
                cfg.files.out_snp_peak_overlaps,
                sep='\t'
            )
        )

    
    return 0

if __name__ == '__main__':
  main()
