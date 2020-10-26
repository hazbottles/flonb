import inspect
from typing import Dict, Tuple, Optional

import dask


def task_func(cache_disk=False):
    """Decorator to convert function to a `flonb.Task`"""

    def decorator(func) -> Task:
        return Task(func, cache_disk=cache_disk)

    return decorator


class Task:
    def __init__(self, func, cache_disk, internal_options: Optional[dict] = None):
        # TODO: pass in func, deps, args?? decorator constructs class?
        # In case we want people to be able to directly construct a Task?
        self.func = func
        self.cache_disk = cache_disk
        self.internal_options = {} if internal_options is None else internal_options

        shallow_option_names = []
        deps = {}
        options_order = []
        for param_name, param_obj in inspect.signature(func).parameters.items():
            options_order.append(param_name)
            default_val = param_obj.default
            if isinstance(default_val, Task):
                deps[param_name] = default_val
            elif (default_val is not inspect.Parameter.empty) and callable(default_val):
                deps[param_name] = default_val
                shallow_option_names += list(inspect.signature(default_val).parameters)
            else:
                shallow_option_names.append(param_name)

        self.options_order = tuple(options_order)
        self.deps = deps
        self.shallow_option_names = shallow_option_names
        self.__name__ = func.__name__

    def __call__(self, *args, **kwargs):
        return self.func(*args, **kwargs)

    def partial(self, **kwargs):
        return Task(self.func, self.cache_disk, internal_options=kwargs)

    def graph(self, **options):
        graph = {}  # singleton that is built throughout recurive calls
        used_options = _build_graph(self, options, graph)

        excess_options = set(options) - set(used_options)
        if excess_options:
            raise ValueError(f"Excess options supplied: {sorted(excess_options)}.")
        return graph

    def compute(self, **options):
        graph = self.graph(**options)
        key = _get_graph_key(self, options)
        return dask.get(graph, key)


def _get_graph_key(task: Task, options: Dict) -> Tuple[str]:
    graph_key = [task.__name__]
    for key in sorted(options):
        graph_key.append(f"{key}={options[key]}")
    return tuple(graph_key)


def _build_graph(task: Task, options: dict, graph: dict):
    # See https://docs.dask.org/en/stable/graphs.html
    # build an s-expression, e.g.
    # (task.func, arg1_key, arg2_key)
    s_expr = [task.func]
    used_options = {}
    available_options = {**options, **task.internal_options}
    for opt in task.options_order:

        # e.g. {("no_of_snowballs", "no_of_snowballs=10"): 10}
        if opt in task.shallow_option_names:
            # _add_option_to_s_expr_and_graph
            opt_val = available_options[opt]
            opt_graph_key = (opt, f"{opt}={opt_val}")
            s_expr.append(opt_graph_key)
            if opt_graph_key not in graph:
                graph[opt_graph_key] = opt_val
            if opt not in task.internal_options:
                used_options[opt] = opt_val

        elif opt in task.deps:
            # step through all the deps,
            # recursively calling `_build_graph` (the function we are in right now!)
            dep_used_options, dep_keys = _add_deps_to_graph(
                task.deps[opt], available_options, graph
            )
            s_expr.append(dep_keys)
            used_options.update(
                {
                    k: v
                    for k, v in dep_used_options.items()
                    if k not in task.internal_options
                }
            )

        else:
            raise ValueError(f"Option '{opt}' unrecognised.")

    task_graph_key = _get_graph_key(task, used_options)
    graph[task_graph_key] = tuple(s_expr)

    return used_options


def _add_deps_to_graph(deps, options: dict, graph: dict):
    """Step recursively down through the depencies.
    Calls `_build_graph` on each Task.
    Replaces Tasks in deps with their graph keys, to build the s-expression.
    """
    if isinstance(deps, Task):
        used_options = _build_graph(deps, options, graph)
        return used_options, _get_graph_key(deps, used_options)
    elif callable(deps):  # dynamic dep
        dynamic_dep_arg_names = list(inspect.signature(deps).parameters)
        deps = deps(**{opt: options[opt] for opt in dynamic_dep_arg_names})
        return _add_deps_to_graph(deps, options, graph)
    elif isinstance(deps, list):
        used_options = {}
        s_expr = []
        for d in deps:
            this_dep_used_opts, this_dep_graph_key = _add_deps_to_graph(
                d, options, graph
            )
            used_options.update(this_dep_used_opts)
            s_expr.append(this_dep_graph_key)
        return used_options, s_expr
    else:
        return {}, deps
