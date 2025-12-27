import pandas as pd

def flatten_json(obj, parent_key="", sep="."):
    items = []

    if isinstance(obj, dict):
        for k, v in obj.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            items.extend(flatten_json(v, new_key, sep=sep).items())

    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            new_key = f"{parent_key}{sep}{i}"
            items.extend(flatten_json(v, new_key, sep=sep).items())

    else:
        items.append((parent_key, obj))

    return dict(items)


def json_to_dataframe(data):
    if isinstance(data, list):
        return pd.DataFrame([flatten_json(item) for item in data])

    if isinstance(data, dict):
        return pd.DataFrame([flatten_json(data)])

    raise ValueError("Unsupported JSON structure")
