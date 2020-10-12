import inspect

from .task import Task


def task_func(cache_disk=False):
    """Decorator to convert function to a `flonb.Task`"""

    def decorator(func) -> TaskFunc:
        return TaskFunc(func, cache_disk=cache_disk)

    return decorator


class TaskFunc:
    def __init__(self, func, cache_disk):
        self.func = func
        self.cache_disk = cache_disk

        param_arg_names = []
        dep_kwargs = {}
        for arg_name, param_obj in inspect.signature(func).parameters.items():
            if inspect.isfunction(param_obj.default):
                dep_kwargs[arg_name] = param_obj.default
            else:
                param_arg_names.append(arg_name)
        self.dep_kwargs = dep_kwargs
        self.param_arg_names = param_arg_names

    def __call__(self, *args, **kwargs):
        return self.func(*args, **kwargs)

    def task(self, params):
        class TaskFromTaskFunc(Task):
            def run(self):
                return self.func(
                    **{params[param_name] for param_name in self.param_arg_names},
                    **self.input,
                )

            def deps(self):
                return {
                    name: _replace_task_func_with_task(dynamic_deps, params)
                    for name, dynamic_deps in self.dep_kwargs.items()
                }

        TaskFromTaskFunc.__name__ = f"{self.func.__name__}Task"

        return TaskFromTaskFunc(params=params, cache_disk=self.cache_disk)

    def compute(self, params):
        return self.task(params).compute()


def _replace_task_func_with_task(dynamic_deps, params):

    # bottom of the recursive calls
    if isinstance(dynamic_deps, TaskFunc):
        return dynamic_deps.task(params)

    kwarg_names = list(inspect.signature(dynamic_deps).parameters)
    deps = dynamic_deps(
        **{kwarg_name: params[kwarg_name] for kwarg_name in kwarg_names}
    )
    if isinstance(deps, dict):
        return {
            arg_name: _replace_task_func_with_task(sub_dep, params)
            for arg_name, sub_dep in deps.items()
        }
    elif isinstance(deps, list):
        return [_replace_task_func_with_task(sub_dep, params) for sub_dep in deps]
    else:
        raise NotImplementedError(
            "Allowed types in dynamic deps are: flonb.TaskFunc; list; dict."
        )
