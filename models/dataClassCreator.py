import os
import keyword
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Union

def sanitize_name(name: str) -> str:
    """Ensure the name is a valid Python identifier."""
    name = ''.join(c if c.isalnum() else '_' for c in name)
    if keyword.iskeyword(name) or not name:
        name += "_cls"
    return name[:1].upper() + name[1:]


def detect_type(value: Any, nested_class_name: Optional[str] = None, is_list=False) -> str:
    """Return the Python type annotation for a value."""
    if value is None:
        return "Optional[Any]"
    elif isinstance(value, bool):
        return "Optional[bool]"
    elif isinstance(value, int):
        return "Optional[int]"
    elif isinstance(value, float):
        return "Optional[float]"
    elif isinstance(value, str):
        return "Optional[str]"
    elif isinstance(value, dict):
        return f"Optional[{nested_class_name or 'Any'}]"
    elif isinstance(value, list):
        if not value:
            return "Optional[List[Any]]"
        first_item = value[0]
        if isinstance(first_item, dict):
            return f"Optional[List[{nested_class_name or 'Any'}]]"
        else:
            return f"Optional[List[{type(first_item).__name__}]]"
    else:
        return f"Optional[{type(value).__name__}]"

def generate_dataclass(
    name: str,
    data: Dict,
    output_dir: str = "./models/generated",
    prepend_parent: bool = False,
    nested_in_file: bool = True
):
    """
    Generate Python dataclasses for a dictionary, including nested dicts/lists.
    - name: name of the top-level class
    - data: dictionary to convert
    - output_dir: where to save files
    - prepend_parent: prepend parent name to nested classes in case of conflict
    - nested_in_file: if True, nested classes go in the same file; else, separate files
    """
    os.makedirs(output_dir, exist_ok=True)
    top_class_name = sanitize_name(name)
    generated_classes = {}  # classname -> properties
    imports_needed = {"dataclass": True, "Optional": True, "List": False, "nested": set()}

    def process_dict(class_name: str, d: dict, parent_name: Optional[str] = None):
        props = {}
        for key, value in d.items():
            is_list = isinstance(value, list)
            nested_class = None

            # Detect nested dicts
            if isinstance(value, dict):
                nested_class_name = sanitize_name(f"{parent_name}_{key}" if prepend_parent and parent_name else key)
                nested_class = nested_class_name
                props[key] = detect_type(value, nested_class_name)
                # Recurse for nested class
                nested_props = process_dict(nested_class_name, value, parent_name=class_name)
                generated_classes[nested_class_name] = nested_props
                imports_needed["nested"].add(nested_class_name)

            elif isinstance(value, list) and value and isinstance(value[0], dict):
                nested_class_name = sanitize_name(f"{parent_name}_{key[:-1]}" if prepend_parent and parent_name else key[:-1])
                nested_class = nested_class_name
                props[key] = f"Optional[List[{nested_class_name}]]"
                imports_needed["List"] = True
                # Recurse for nested class
                nested_props = process_dict(nested_class_name, value[0], parent_name=class_name)
                generated_classes[nested_class_name] = nested_props
                imports_needed["nested"].add(nested_class_name)

            else:
                props[key] = detect_type(value)
                if is_list:
                    imports_needed["List"] = True

        return props

    # Build all class property dictionaries
    top_props = process_dict(top_class_name, data)
    generated_classes[top_class_name] = top_props

    # Write classes to file(s)
    for cls_name, props in generated_classes.items():
        file_name = os.path.join(output_dir, f"{cls_name}.py") if not nested_in_file or cls_name != top_class_name else os.path.join(output_dir, f"{top_class_name}.py")
        existing_props = {}
        if os.path.exists(file_name):
            with open(file_name, "r") as f:
                lines = f.readlines()
            for line in lines:
                if "=" in line and ":" in line:
                    k = line.split(":")[0].strip()
                    existing_props[k] = line

        with open(file_name, "w") as f:
            # imports
            f.write("from dataclasses import dataclass\n")
            import_list = []
            if imports_needed.get("Optional"): import_list.append("Optional")
            if imports_needed.get("List"): import_list.append("List")
            if import_list:
                f.write(f"from typing import {', '.join(import_list)}\n")
            for nested_cls in sorted(imports_needed["nested"]):
                if nested_in_file and nested_cls != top_class_name:
                    f.write(f"from .{nested_cls} import {nested_cls}\n")
            f.write("\n@dataclass\n")
            f.write(f"class {cls_name}:\n")
            if not props:
                f.write("    pass\n")
            else:
                for k, t in props.items():
                    if k not in existing_props:
                        f.write(f"    {k}: {t} = None\n")

    print(f"Dataclass(es) generated for {name} in {output_dir}")

