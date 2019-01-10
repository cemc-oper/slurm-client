# coding: utf-8

try:
    import pyslurm
    HAS_PYSLURM = True
except Exception:
    HAS_PYSLURM = False
