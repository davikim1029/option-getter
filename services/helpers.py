import inspect
import importlib
import builtins

def snapshot_module(module, capture_prints=True):
    """Show a detailed snapshot of a loaded module, including functions, classes, variables, and optionally captures top-level prints."""
    output_lines = []
    
    # Capture top-level prints if requested
    captured_prints = []
    if capture_prints:
        old_print = builtins.print
        def fake_print(*args, **kwargs):
            captured_prints.append(" ".join(str(a) for a in args))
            old_print(*args, **kwargs)
        builtins.print = fake_print

    try:
        # Reload the module
        importlib.reload(module)
    finally:
        if capture_prints:
            builtins.print = old_print

    output_lines.append(f"=== MODULE SNAPSHOT: {module.__name__} ===\n")
    
    # Module source
    try:
        source = inspect.getsource(module)
        output_lines.append("---- Source Code ----")
        output_lines.append(source)
    except Exception:
        output_lines.append("Source not available")

    # Variables
    output_lines.append("\n---- Module-level Variables ----")
    for name, obj in vars(module).items():
        if name.startswith("__") and name.endswith("__"):
            continue
        if inspect.isfunction(obj) or inspect.isclass(obj):
            continue
        try:
            output_lines.append(f"{name} = {repr(obj)}")
        except Exception:
            output_lines.append(f"{name} = <unrepr-able object>")

    # Functions
    output_lines.append("\n---- Functions ----")
    for name, obj in inspect.getmembers(module, inspect.isfunction):
        output_lines.append(f"{name}{inspect.signature(obj)}")

    # Classes
    output_lines.append("\n---- Classes ----")
    for name, obj in inspect.getmembers(module, inspect.isclass):
        output_lines.append(f"class {name}")
        for mname, method in inspect.getmembers(obj, inspect.isfunction):
            output_lines.append(f"    {mname}{inspect.signature(method)}")

    # Captured prints
    if capture_prints and captured_prints:
        output_lines.append("\n---- Top-level prints captured during reload ----")
        output_lines.extend(captured_prints)

    # Final print
    final_snapshot = "\n".join(output_lines)
    print(final_snapshot)
    return final_snapshot
