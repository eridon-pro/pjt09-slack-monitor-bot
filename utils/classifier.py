"""
モック分類ロジック
- "badword" を含むと violation=True
- "ありがとう" を含むと positive=True
- それ以外は neutral
"""
def classify_text(text: str) -> dict:
    txt = text.lower()
    if "badword" in txt:
        return {"violation": True,  "positive": False}
    if "ありがとう" in txt:
        return {"violation": False, "positive": True}
    # neutral
    return {"violation": False, "positive": False}
