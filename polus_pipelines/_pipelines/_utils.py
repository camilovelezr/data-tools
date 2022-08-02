def _filter(t: object, l: list):
    return list(filter(lambda x: isinstance(x, t), l))


def _add_to_dict(d: dict, kv: tuple) -> None:
    """Update vector of value if key exists. Create key if new."""
    if kv[1] in d:
        d[kv[1]].append(kv[0])
    else:
        d[kv[1]] = [kv[0]]
