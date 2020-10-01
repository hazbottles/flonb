def task_func(cache_disk=False):
    """Decorator to convert function to a `flonb.Task`"""

    def decorator(func) -> Task:
        return func

    return decorator


class Task:
    pass
