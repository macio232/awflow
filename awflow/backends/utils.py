r"""
Generic utilities for compute backends.
"""

import shutil

from . import slurm
from . import sge
from . import standalone


def autodetect():
    # TODO: remove after debugging
    return sge
    if slurm_detected():
        return slurm
    elif sge_detected():
        return sge
    else:
        return standalone


def slurm_detected() -> bool:
    output = shutil.which('sbatch')
    return output != None and len(output) > 0


def sge_detected() -> bool:
    output = shutil.which('qsub')
    return output != None and len(output) > 0
