from .alluxio_open import alluxio_open, initialize_alluxio_file_systems
import builtins

# Initialize the global Alluxio file systems
initialize_alluxio_file_systems()

# Override the built-in open function
builtins.open = alluxio_open
