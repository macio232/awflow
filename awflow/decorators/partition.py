r"""
Decorators specific to the Slurm partition plugin.
"""
import awflow
from awflow.node import Node
from awflow.plugins.partition import set_partitions_slurm, set_constraint, set_partitions_sge
from awflow.utils.dawg import add_and_get_node
from awflow.utils.decorator import parameterized
from awflow.utils.generic import is_iterable

from typing import List
from typing import Callable
from typing import Union


@parameterized
def partition(f: Callable, partition: Union[str, List[str]]) -> Callable:
    if awflow.backend.__backend__ == 'slurm':
        node = add_and_get_node(f)
        if not is_iterable(partition):
            partition = [partition]
        set_partitions_slurm(node, partition)
    elif awflow.backend.__backend__ == 'sge':
        node = add_and_get_node(f)
        if not is_iterable(partition):
            partition = [partition]
        set_partitions_sge(node, partition)
    return f


@parameterized
def constraint(f: Callable, constraint: Union[str, List[str]]) -> Callable:
    if awflow.backend.__backend__ == 'slurm':
        node = add_and_get_node(f)
        if isinstance(constraint, list):
            if len(constraint) > 1:
                constraint = "|".join(constraint)
            elif len(constraint) == 1:
                constraint = constraint[0]
            else:
                constraint = None
        set_constraint(node, constraint)

    return f
