import awflow
import os

from awflow.dawg import DirectedAcyclicWorkflowGraph as DAWG
from awflow.node import Node

from typing import List


def apply_defaults(node: Node, **kwargs) -> None:
    # Only apply to Slurm backend
    if awflow.backend.__backend__ == 'slurm':
        partition = kwargs.get('partition', None)
        if partition is not None:
            if not isinstance(partition, list):
                partition = [partition]
            set_partitions_slurm(node, partition)
    elif awflow.backend.__backend__ == 'sge':
        partition = kwargs.get('partition', None)
        if partition is not None:
            if not isinstance(partition, list):
                partition = [partition]
            set_partitions_sge(node, partition)
    else:
        return


def set_partitions_slurm(node: Node, partitions: List[str]) -> None:
    if '--partition=' not in node.attributes.keys():
        node['--partition='] = ','.join(partitions)


def set_partitions_sge(node: Node, partitions: List[str]) -> None:
    if '-jc' not in node.attributes.keys():
        node['-jc'] = os.environ.get('SGE_CPU_JOB_CLASS', 'pcc-normal')


def set_constraint(node: Node, constraints: str) -> None:
    if constraints is not None:
        if '--constraint=' not in node.attributes.keys():
            node['--constraint='] = constraints


def generate_before(node: Node) -> List[str]:
    return []


def generate_after(node: Node) -> List[str]:
    return []
