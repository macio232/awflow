r"""
Decorator implementation for the standalone backend.
"""

from __future__ import annotations
import os
import re

from awflow.node import Node

from typing import Callable


def cpus(node: Node, n: int) -> None:
    node['-pe'] = f"{os.environ.get('SGE_PARALLEL_ENVIRONMENT', 'OpenMP')} {str(n)}"


def memory(node: Node, memory: str, n: int = 1) -> None:
    pass


def gpus(node: Node, n: int, memory: str=None) -> None:
    if n != 0:
        node['-jc'] = os.environ.get('SGE_GPU_JOB_CLASS', 'gpu-container_g1.24h')
        node['-ac'] = f"d={os.environ.get('SGE_BASE_CONTAINER', 'nvcr-cuda-11.7.0-ubuntu20.04')}"


def timelimit(node: Node, timelimit: str) -> None:
    pass
