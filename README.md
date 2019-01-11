# Slurm Client

A cli client for Slurm in NWPC.

## Features

  - Query active jobs.
  - Show partition information.

## Installing

Install `nwpc-hpc-model` package. You can download this package from Github or
use the shadow version in `nwpc-nost`'s vendor directory.

Install `slurm-client` package using `pip install .`.

## Getting started

Query Slurm jobs:

```bash
slurm_client query
```

All jobs running or waiting in Slurm will be shown:

```
$slclient query
5539723 RUNNING normal    cra_op   12/30 08:21 /g2/cra_op/stream_plot/JOB
5539975 RUNNING serial    das_xp   12/30 09:17 /g3/das_xp/zhanglin/exp3/GRAPES_GFS2.3-20181229/RUN/run.cmd
5571157 RUNNING serial    zhanghua 12/31 04:54 /g3/zhanghua/exph/GRAPES_GFS_2-1-2-2/RUN/run.cmd
5572669 RUNNING serial    wangrch  12/31 05:09 /g3/wangrch/gen_be/NMC/fcst/p_run_warm_NCEP_00.sh
```

Use `slclient query --help` to see options.

Use `slclient --help` to see more sub-commands.

## Config

`slurm-client` use a YAML file to set categories which are extracted from Slurm `squeue` command. Such as

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

Default config file is installed. To use a different config, use `--config-file` parameter.

```bash
slurm_client query --config-file=some/config/path
```

## LICENSE

Copyright &copy; 2018-2019, Perilla Roc.

`slurm-client` is licensed under [GPL-3.0](#).

[GPL-3.0]: http://www.gnu.org/licenses/gpl-3.0.en.html