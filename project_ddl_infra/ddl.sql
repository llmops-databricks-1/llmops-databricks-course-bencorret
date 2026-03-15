create catalog if not exists llmops_dev;
create schema if not exists llmops_dev.global_findex;
create volume if not exists llmops_dev.global_findex.global_findex_files;

create catalog if not exists llmops_acc;
create schema if not exists llmops_acc.global_findex;
create volume if not exists llmops_acc.global_findex.global_findex_files;

create catalog if not exists llmops_prd;
create schema if not exists llmops_prd.global_findex;
create volume if not exists llmops_prd.global_findex.global_findex_files;