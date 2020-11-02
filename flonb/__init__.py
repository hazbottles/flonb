from .task import task_func, set_cache_dir, Dep, DynamicDep  # noqa: F401

__version__ = "0.1.0"  # make sure to also update in ../setup.py

__all__ = ["task_func", "set_cache_dir", "Dep", "DynamicDep", "__version__"]
