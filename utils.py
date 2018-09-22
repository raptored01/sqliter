def scrub(text):
    return "".join(c for c in text if c.isalnum() or c is "_")


def clean_kwargs(**kwargs):
    cleaned_kwargs = {}
    for key, value in kwargs.items():
        cleaned_key = scrub(key)
        cleaned_kwargs[cleaned_key] = value
    return cleaned_kwargs


def is_valid_field_name(text):
    return not text.startswith("__")
