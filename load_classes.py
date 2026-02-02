import re
import sys
import importlib
import pkgutil
import inspect


class Loader():

    def __init__(self, path, name_module):
        self.path = path
        self.name_module = name_module

    def load_classes(self, start_date):
        classes = []
        # Iterate over all modules in the current package
        for loader, module_name, is_pkg in pkgutil.iter_modules(self.path):
            # Dynamically import the module
            module = importlib.import_module(f"{self.name_module}.{module_name}")
            
            # Inspect module members and import classes into package namespace
            for name, obj in inspect.getmembers(module, inspect.isclass):
                # Ensure the class is defined in this module (not imported from elsewhere)
                if obj.__module__ == module.__name__:
                    name_var = re.sub(r'(?<!^)(?=[A-Z])', '_', name).lower()
                    #exec(f'{name_var} = obj(valor)', {'obj': obj, 'valor': valor})
                    exec(f'self.{name_var} = obj(start_date)')
                    #globals()[name] = obj
                    #classes.append(name)
