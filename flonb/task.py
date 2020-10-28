import inspect
from typing import Dict, Tuple, Optional

import dask


def task_func(cache_disk=False):
    """Decorator to convert function to a `flonb.Task`"""

    def decorator(func) -> Task:
        return Task(func, cache_disk=cache_disk)

    return decorator


class Task:
    def __init__(self, func, cache_disk, presupplied_options: Optional[dict] = None):
        # TODO: pass in func, deps, args?? decorator constructs class?
        # In case we want people to be able to directly construct a Task?
        self.__module__ = func.__module__
        self.__name__ = func.__name__
        self.__qualname__ = func.__qualname__
        self.__doc__ = (
            f"This is a `flonb.Task` wrapping function {func}. Original function docstring:\n"
            "--------------\n\n"
            f"{func.__doc__}"
        )
        self.__signature__ = inspect.signature(func)

        self.func = func
        self.cache_disk = cache_disk
        self.presupplied_options = (
            {} if presupplied_options is None else presupplied_options
        )

        shallow_option_names = []
        deps = {}
        args_order = []
        for param_name, param_obj in inspect.signature(func).parameters.items():
            args_order.append(param_name)
            default_val = param_obj.default
            if isinstance(default_val, Task):
                deps[param_name] = default_val
            elif (default_val is not inspect.Parameter.empty) and callable(default_val):
                deps[param_name] = default_val
                shallow_option_names += list(inspect.signature(default_val).parameters)
            else:
                shallow_option_names.append(param_name)

        self.args_order = tuple(args_order)
        self.deps = deps
        self.shallow_option_names = shallow_option_names

        for opt, val in self.presupplied_options.items():
            if opt not in self.shallow_option_names:
                raise ValueError(
                    f"Pre-supplied option '{opt}'={val} is not a valid option for '{self.__name__}'."
                )

    def __call__(self, *args, **kwargs):
        return self.func(*args, **kwargs)

    def __repr__(self):
        return (
            f"flonb.Task            {self.__name__}\n"
            f"func:                 {self.func}\n"
            f"cache_disk:           {self.cache_disk}\n"
            f"presupplied_options:  {self.presupplied_options}\n"
            f"shallow option args:  {self.shallow_option_names}\n"
            f"dependency args:      {list(self.deps)}"
        )

    def partial(self, **kwargs):
        return Task(self.func, self.cache_disk, presupplied_options=kwargs)

    def graph(self, **options):
        graph = {}  # singleton that is built throughout recursive calls
        used_options, key = _build_graph(self, options, graph)

        excess_options = set(options) - set(used_options)
        if excess_options:
            raise ValueError(f"Excess options supplied: {sorted(excess_options)}.")
        return graph, key

    def compute(self, **options):
        graph, key = self.graph(**options)
        return dask.get(graph, key)


def _get_graph_key(task: Task, options: Dict) -> Tuple[str]:
    graph_key = [task.__name__]
    for key in sorted(options):
        graph_key.append(f"{key}={options[key]}")
    return tuple(graph_key)


def _get_option(options: dict, opt: str):
    """Dictionary look-up with flonb specific error message"""
    if opt in options:
        return options[opt]
    raise ValueError(f"Missing option '{opt}'.")


def _build_graph(task: Task, options: dict, graph: dict):
    # See https://docs.dask.org/en/stable/graphs.html
    # build an s-expression, e.g.
    # (task.func, arg1_key, arg2_key)
    s_expr = [task.func]
    used_options = {}
    twice_specified_options = set(options) & set(task.presupplied_options)
    if twice_specified_options:
        raise ValueError(
            f"Options {sorted(twice_specified_options)} "
            f"have already been pre-supplied to '{task.__name__}'."
        )
    available_options = {**options, **task.presupplied_options}
    for arg in task.args_order:

        # e.g. {("no_of_snowballs", "no_of_snowballs=10"): 10}
        if arg in task.shallow_option_names:
            # _add_option_to_s_expr_and_graph
            opt_val = _get_option(available_options, arg)
            opt_graph_key = (arg, f"{arg}={opt_val}")
            s_expr.append(opt_graph_key)
            if opt_graph_key not in graph:
                graph[opt_graph_key] = opt_val
            if arg not in task.presupplied_options:
                used_options[arg] = opt_val

        elif arg in task.deps:
            # step through all the deps,
            # recursively calling `_build_graph` (the function we are in right now!)
            dep_used_options, dep_keys = _add_deps_to_graph(
                task.deps[arg], available_options, graph
            )
            s_expr.append(dep_keys)
            used_options.update(
                {
                    k: v
                    for k, v in dep_used_options.items()
                    if k not in task.presupplied_options
                }
            )

        else:
            raise RuntimeError(f"Internal Error - argument '{arg}' unconfigured.")

    graph_key = _get_graph_key(task, {**used_options, **task.presupplied_options})
    graph[graph_key] = tuple(s_expr)

    return used_options, graph_key


def _add_deps_to_graph(deps, options: dict, graph: dict):
    """Step recursively down through the depencies.
    Calls `_build_graph` on each Task.
    Replaces Tasks in deps with their graph keys, to build the s-expression.
    """
    if isinstance(deps, Task):
        return _build_graph(deps, options, graph)
    elif callable(deps):  # dynamic dep
        dynamic_dep_option_names = list(inspect.signature(deps).parameters)
        deps = deps(
            **{opt: _get_option(options, opt) for opt in dynamic_dep_option_names}
        )
        used_options, graph_key = _add_deps_to_graph(deps, options, graph)
        used_options.update({k: options[k] for k in dynamic_dep_option_names})
        return used_options, graph_key
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
