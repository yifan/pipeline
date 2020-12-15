import importlib


def import_class(importString):
    moduleName, _, className = importString.partition(":")

    try:
        module = importlib.import_module(moduleName)
    except ImportError as e:
        if e.name != moduleName:
            raise e from None
        raise Exception(f"Could not import module {moduleName}")

    try:
        instance = getattr(module, className)
    except AttributeError:
        raise Exception(f"Class {className} not found in module {moduleName}")

    return instance
