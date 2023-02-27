r"""
Decorator implementation for the standalone backend.
"""

from __future__ import annotations
import re

from awflow.node import Node

from typing import Callable



def cpus(node: Node, n: int) -> None:
    # TODO: (Fixed allocation_rule in PE)
    raise NotImplementedError
    node['--cpus-per-task='] = str(n)


def memory(node: Node, memory: str, n: int = 1) -> None:
    mem = float(re.match(r"\d+", memory).group(0)) / n
    node['-l h_vmem='] = f'{mem:.2f}G'


def gpus(node: Node, n: int, memory: str=None) -> None:
    raise NotImplementedError
    if memory is None:
        node['--gres=gpu:'] = str(n)
    else:
        node[f'--gres=gpu:{str(n)},VramPerGpu:{memory.upper()}'] = ''


def timelimit(node: Node, timelimit: str) -> None:
    node['-l h_rt='] = str(timelimit)
