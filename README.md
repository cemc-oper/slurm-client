# Slurm Client

A cli client for Slurm in NWPC.

## Installing

Install `nwpc-hpc-model` package. You can download this package from Github or
use the shadow version in `nwpc-operation-system-tool`'s vendor directory.

Install `slurm-client` package using `python setup.py install`.

## Getting started

Query slurm jobs:

```bash
slurm_client query
```

All jobs running or waiting in Slurm will be shown:

```
$slclient query
1133068 RUNNING operation nwp_pd 08/30 01:32 /g2/nwp_pd/ECFLOWOUT/3km_post/06/3km_togrib2/grib2WORK/004/data2grib2_004.job1
1133069 RUNNING operation nwp_pd 08/30 01:32 /g2/nwp_pd/ECFLOWOUT/3km_post/06/3km_togrib2/grib2WORK/005/data2grib2_005.job1
1133070 RUNNING operation nwp_pd 08/30 01:32 /g2/nwp_pd/ECFLOWOUT/3km_post/06/3km_togrib2/grib2WORK/006/data2grib2_006.job1
1133064 RUNNING operation nwp_pd 08/30 01:32 /g2/nwp_pd/ECFLOWOUT/3km_post/06/3km_togrib2/grib2WORK/000/data2grib2_000.job1
1133065 RUNNING operation nwp_pd 08/30 01:32 /g2/nwp_pd/ECFLOWOUT/3km_post/06/3km_togrib2/grib2WORK/001/data2grib2_001.job1
1133066 RUNNING operation nwp_pd 08/30 01:32 /g2/nwp_pd/ECFLOWOUT/3km_post/06/3km_togrib2/grib2WORK/002/data2grib2_002.job1
```


Default config file is installed. To use a different config, use `--config-file` parameter.

```bash
slurm_client query --config-file=some/config/path
```

## Config

`slurm-client` use a YAML file to set catogries which are extract from Slurm `squeue` command. Such as

```yaml
squeue:
  category_list:
    -
      id: "squeue.job_id"
      display_name: "Job Id"
      label: "JOBID"
      record_parser_class: "TokenRecordParser"
      record_parser_arguments:
        - -1
        - "|"
      value_saver_class: "StringSaver"
      value_saver_arguments: []
    -
      id: "squeue.account"
      display_name: "Account"
      label: "ACCOUNT"
      record_parser_class: "TokenRecordParser"
      record_parser_arguments:
        - -1
        - "|"
      value_saver_class: "StringSaver"
      value_saver_arguments: []
```
