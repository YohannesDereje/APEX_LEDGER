import hashlib


def generate_content_hash(text: str, page_number: int, bbox) -> str:
    seed = f"{text}|{page_number}|{bbox.x1},{bbox.y1},{bbox.x2},{bbox.y2}"
    return hashlib.sha256(seed.encode("utf-8")).hexdigest()