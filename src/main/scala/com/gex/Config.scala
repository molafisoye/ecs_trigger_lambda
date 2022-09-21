package com.gex

object Config {
    val clusterName: String = System.getenv("CLUSTER_NAME")
    val securityGroup: String = System.getenv("SECURITY_GROUP")
    val subnet: String = System.getenv("SUBNET")
    val umitoolsExtractTaskDefFamily: String = System.getenv("UMITOOLS_EXTRACT_TASK_DEF_NAME")
    val umitoolsDedupTaskDefFamily: String = System.getenv("UMITOOLS_DEDUP_TASK_DEF_NAME")
    val htseqCountTaskDefFamily: String = System.getenv("HTSEQ_COUNT_TASK_DEF_NAME")
    val samtoolsIndexTaskDefFamily: String = System.getenv("SAMTOOLS_TASK_DEF_NAME")
    val bbdukTaskDefFamily: String = System.getenv("BBDUK_EXTRACT_TASK_DEF_NAME")
    val starTaskDefFamily: String = System.getenv("STAR_EXTRACT_TASK_DEF_NAME")
    val fastqcTaskDefFamily: String = System.getenv("FASTQC_EXTRACT_TASK_DEF_NAME")
}
