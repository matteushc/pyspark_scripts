

# Automatically import all classes from all modules in this package
#__all__ = []  # Will store all public class names

from utils.load_classes import Loader

default_loader = Loader(__path__, __name__)

__all__ = ["Loader", "default_loader"]

# Iterate over all modules in the current package
# for loader, module_name, is_pkg in pkgutil.iter_modules(__path__):
#     # Dynamically import the module
#     module = importlib.import_module(f"{__name__}.{module_name}")
    
#     # Inspect module members and import classes into package namespace
#     for name, obj in inspect.getmembers(module, inspect.isclass):
#         # Ensure the class is defined in this module (not imported from elsewhere)
#         if obj.__module__ == module.__name__:
#             name_var = re.sub(r'(?<!^)(?=[A-Z])', '_', name).lower()
#             obj("ola")
#             globals()[name] = obj
#             __all__.append(name)
