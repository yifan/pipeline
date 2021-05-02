import importlib
from typing import Any


def import_class(import_string: str) -> Any:
    module_name, _, class_name = import_string.partition(":")

    try:
        module = importlib.import_module(module_name)
    except ImportError as e:
        if e.name != module_name:
            raise e from None
        raise Exception(f"Could not import module {module_name}")

    try:
        instance = getattr(module, class_name)
    except AttributeError:
        raise Exception(f"Class {class_name} not found in module {module_name}")

    return instance
