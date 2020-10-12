from typing import List


class Task:
    def __init__(self, params: dict, cache_disk=True):
        self.all_params = params
        for param in self.shallow_params():
            if param not in self.all_params:
                raise ValueError(f"Param '{param}' is not specified for {self}")

    def shallow_params(self) -> List[str]:
        raise NotImplementedError

    def run(self):
        raise NotImplementedError

    def deps(self):
        raise NotImplementedError

    def _run(self):
        if self.cache_disk:
            raise NotImplementedError
        else:
            self.run()

    def build_dask_graph(self):
        pass

    def compute(self):
        pass
