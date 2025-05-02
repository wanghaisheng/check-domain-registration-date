import pandas as pd

def cleandomain(domain):
    if not isinstance(domain, str):
        domain = str(domain)
    domain = domain.strip()
    if domain.startswith("https://"):
        domain = domain.replace("https://", "")
    if domain.startswith("http://"):
        domain = domain.replace("http://", "")
    if domain.startswith("www."):
        domain = domain.replace("www.", "")
    if domain.endswith("/"):
        domain = domain.rstrip("/")
    return domain

def get_tld(domain: str):
    """Extracts the top-level domain from a domain name."""
    parts = domain.split(".")
    return ".".join(parts[1:]) if len(parts) > 1 else parts[0]

def filter_done_domains(domains, done_domains):
    done_set = set(done_domains)
    return [d for d in domains if d not in done_set]

def standardize_column_names(df: pd.DataFrame, keys: list) -> pd.DataFrame:
    for key in keys:
        if key not in df.columns:
            df[key] = ''
    return df 