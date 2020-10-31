import inspect
import functools
import os
import pickle
from typing import Callable, Dict, Tuple, Optional


import dask
import dask.optimization


def set_cache_dir(dirpath: str):
    Cache.set_dir(dirpath)


def task_func(func=None, *, cache_disk=False):
    """Decorator to convert function to a `flonb.Task`"""

    def decorator(func) -> Task:
        return Task(func, cache_disk=cache_disk)

    if func is None:
        return decorator
    else:
        return decorator(func)


class Cache:
    _base_dirpath: str = None

    def __init__(self, category: str, key: str):
        self.category = category
        self.key = key
        self.fpath = os.path.join(self._get_base_dir(), category, f"{key}.pickle")

    def exists(self) -> bool:
        return os.path.exists(self.fpath)

    def read(self) -> object:
        with open(self.fpath, "rb") as fd:
            return pickle.load(fd)

    def write(self, data: object):
        os.makedirs(os.path.dirname(self.fpath), exist_ok=True)
        with open(self.fpath, "wb") as fd:
            pickle.dump(data, fd)

    @classmethod
    def set_dir(cls, dirpath: str):
        cls._base_dirpath = dirpath

    @classmethod
    def _get_base_dir(cls) -> str:
        if cls._base_dirpath is None:
            raise ValueError("Set cache dir with `flonb.set_cache_dir`.")
        return cls._base_dirpath

    @classmethod
    def _reset(cls):
        cls._base_dirpath = None


class Task:
    def __init__(
        self,
        func: Callable,
        cache_disk: bool,
        presupplied_options: Optional[dict] = None,
    ):
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
            if isinstance(default_val, Dep):
                deps[param_name] = default_val
            elif isinstance(default_val, DynamicDep):
                deps[param_name] = default_val
                shallow_option_names += [
                    opt
                    for opt in default_val.option_names
                    if opt not in shallow_option_names
                ]
            else:
                shallow_option_names.append(param_name)

        self.args_order = tuple(args_order)
        self.deps = deps
        self.shallow_option_names = shallow_option_names

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

    def _get_cache_obj(self, key: str) -> Cache:
        return Cache(self.__name__, key)

    def _get_graph_func(self, key: str) -> Callable:
        """Returns a wrapper around self.func that handles"""
        if not self.cache_disk:
            return self.func

        @functools.wraps(self.func)
        def write_cache_wrapper(*args, **kwargs):
            result = self.func(*args, **kwargs)
            cache = self._get_cache_obj(key)
            cache.write(result)
            return cache.read()

        return write_cache_wrapper

    def _get_cache_read_func(self, key: str) -> Callable:
        def get_cached_data(*args):
            return self._get_cache_obj(key).read()

        return get_cached_data

    def _cache_exists(self, key):
        return self._get_cache_obj(key).exists()

    def partial(self, **options):
        return Task(self.func, self.cache_disk, presupplied_options=options)

    def graph_and_key(self, **options):
        graph = {}  # singleton that is built throughout recursive calls
        used_options, key = _build_graph(self, options, graph)

        excess_options = set(options) - set(used_options)
        if excess_options:
            raise ValueError(f"Excess options supplied: {sorted(excess_options)}.")
        graph, _ = dask.optimization.cull(graph, key)
        return graph, key

    def compute(self, **options):
        graph, key = self.graph_and_key(**options)
        return dask.get(graph, key)


class Dep:
    def __init__(self, dep):
        self.dep = dep

    def __repr__(self):
        dep = self.dep
        dep_str = dep.__name__ if isinstance(dep, Task) else dep
        return f"flonb.Dep({dep_str})"


class DynamicDep:
    def __init__(self, dynamic_dep: Callable):
        self.dynamic_dep = dynamic_dep
        self.option_names = list(inspect.signature(dynamic_dep).parameters)

    def get_dep(self, available_options):
        return Dep(
            self.dynamic_dep(
                **{
                    opt: _get_option(available_options, opt)
                    for opt in self.option_names
                }
            )
        )

    def __repr__(self):
        return f"flonb.DynamicDep(options={self.option_names})"


def _get_graph_key(task: Task, used_options: Dict) -> Tuple[str]:
    option_strs = []
    for key in sorted(used_options):
        option_strs.append(f"{key}={used_options[key]}")
    return tuple([task.__name__, ", ".join(option_strs)])


def _get_option(options: dict, opt: str):
    """Dictionary look-up with flonb specific error message"""
    if opt in options:
        return options[opt]
    raise ValueError(f"Missing option '{opt}'.")


def _build_graph(task: Task, options: dict, graph: dict):
    # See https://docs.dask.org/en/stable/graphs.html
    # build an s-expression, e.g.
    # (task.func, arg1_key, arg2_key)
    s_expr = []
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
            used_options[arg] = opt_val

        elif arg in task.deps:
            # step through all the deps,
            # recursively calling `_build_graph` (the function we are in right now!)
            dep_used_options, dep_keys = _add_deps_to_graph(
                task.deps[arg], available_options, graph
            )
            s_expr.append(dep_keys)
            used_options.update(dep_used_options)

    identifying_options = {**task.presupplied_options, **used_options}
    graph_key = _get_graph_key(task, identifying_options)
    if task.cache_disk and task._cache_exists(graph_key):
        s_expr = [task._get_cache_read_func(graph_key)]
        for opt, opt_val in identifying_options.items():
            s_expr.append((opt, f"{opt}={opt_val}"))
    else:
        s_expr.insert(0, task._get_graph_func(graph_key))
    graph[graph_key] = tuple(s_expr)

    for presupplied_opt, presupplied_opt_val in task.presupplied_options.items():
        if presupplied_opt not in used_options:
            raise ValueError(
                f"Pre-supplied option '{presupplied_opt}'={presupplied_opt_val} "
                f"to task '{task.__name__}' was unused."
            )
        del used_options[presupplied_opt]

    return used_options, graph_key


def _add_deps_to_graph(deps, options: dict, graph: dict):
    """Step recursively down through the depencies.
    Calls `_build_graph` on each Task.
    Replaces Tasks in deps with their graph keys, to build the s-expression.
    """
    if isinstance(deps, Task):
        return _build_graph(deps, options, graph)
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
    elif isinstance(deps, Dep):
        return _add_deps_to_graph(deps.dep, options, graph)
    elif isinstance(deps, DynamicDep):
        used_options, graph_key = _add_deps_to_graph(
            deps.get_dep(options), options, graph
        )
        used_options.update({k: options[k] for k in deps.option_names})
        return used_options, graph_key
    else:
        return {}, deps
