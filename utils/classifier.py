import os
import re
import openai
import json
import logging
logger = logging.getLogger(__name__)

# Default Model overwritten by .env file
#MODEL = "gpt-3.5-turbo"
MODEL = os.environ.get("OPENAI_MODEL", "gpt-3.5-turbo")

# ポジティブフィードバック検出用キーワード
POSITIVE_KEYWORDS = ["ありがとう", "感謝", "助かった", "すごい", "素晴らしい", "ナイス", "thanks", "thank you", "thx",]

# ポジティブリアクション（絵文字キー）
POSITIVE_REACTIONS = {
    '+1', 'thumbsup',    # 👍
    'heart',             # ❤️
    'tada',              # 🎉
    'clap',              # 👏
    'raised_hands',      # 🙌
    'bow',               # 🙇
    'bowing_woman',      # 🙇‍♀️
    'bowing_man',        # 🙇‍♂️
    'pray',              # 🙏 
}

# ネガティブ判定排除キーワード
NEGATIVE_ANSWER_KEYWORDS = ["わからない", "知らない", "できません", "どうでしょう", "先生？"]

# 読み込み：コミュニティガイドライン
BASE_DIR = os.path.dirname(__file__)
with open(os.path.join(BASE_DIR, "guidelines.txt"), encoding="utf-8") as f:
    GUIDELINES = f.read()

# 項目番号をパースして最大値を取得
# 1. 各行の先頭にある「数字.」を全部抜き出して数値化
numbers = [int(m.group(1))
           for m in re.finditer(r'^\s*(\d+)\.', GUIDELINES, re.MULTILINE)]
# 2. 最大の番号が項目数
NUM_GUIDELINES = max(numbers) if numbers else 0

# 番号付き規約テキストをマップ化
RULES_MAP = {
    int(m.group(1)): m.group(2).strip()
    for m in re.finditer(r'^\s*(\d+)\.\s*(.+)$', GUIDELINES, re.MULTILINE)
}

def classify_text(text: str) -> dict:
    """
    ガイドライン違反判定を LLM で行い、辞書で返す。
    {"violation": bool}
    モック fallback も含む。
    """
    # モック版: badword があれば違反
    if "badword" in text.lower():
        return {"violation": True}
    # LLM 判定（例: ChatGPT）
    if openai.api_key:
        # システムプロンプトに規約を埋め込み、ユーザープロンプトで投稿を渡す
        messages = [
            {
                "role": "system",
                "content": (
                    "あなたは研究室のSlackコミュニティ運営ボットです。以下はコミュニティ規約(番号付き)です。全文をよく読み、"
                    "投稿が規約違反かどうか、かつ、違反なら何番に違反しているかを番号で答えてください。\n\n"
                    f"{GUIDELINES}"
                )
            },
            {
                "role": "user",
                "content": (
                    f"次の投稿について:\n```{text}```\n"
                    "1) 違反していますか？Yes/No\n"
                    "2) 違反なら、違反した規約番号をカンマ区切りで教えてください。違反がない場合は、番号は一切返さないでください。"
                    #f"次のSlack投稿がコミュニティ規約に違反しているか？ Yes か No で答えてください。\n```{text}```"
                )
            }
        ]
        create_args = {"model": MODEL, "messages": messages,}
        logger.info(f"[LLM: {MODEL}] classify_text req: '{text[:80]}'")
        # gpt-3.5-turbo 系で temperature=0 を使いたい場合
        if not MODEL.startswith("o4-"):
            create_args["temperature"] = 0
        
        resp = openai.chat.completions.create(**create_args)
        out = resp.choices[0].message.content.strip()
        logger.info(f"[LLM: {MODEL}] classify_text resp: '{out[:80]}'")
        # 返り値例:
        # Yes
        # 3,5
        text_lower = out.lower()
        violation = bool(re.search(r"\byes\b", text_lower))  # Yes/No 判定
        # 違反ありの場合のみ番号を抽出、それ以外は空リスト
        if violation:
            # 全文から番号を抽出 → セット化して重複を除き、ソート
            extracted = map(int, re.findall(r"\b[1-9]\d*\b", out))
            valid_rules = sorted({
                n for n in extracted
                if 1 <= n <= NUM_GUIDELINES
            })
        else:
            valid_rules = []
        return {"violation": violation, "rules": valid_rules}
    
    # API キーがない場合のフォールバック
    return {"violation": False}

'''
def detect_positive_feedback(text: str) -> list:
    """
    他ユーザーへの感謝・賞賛のメンションを検出し、対象ユーザーIDリストを返す
    """
    if not any(kw in text for kw in POSITIVE_KEYWORDS):
        return []
    # メンション形式 <@U12345>
    ids = re.findall(r"<@([A-Z0-9]+)>", text)
    return ids
'''

def detect_positive_feedback(text: str) -> list[str]:
    """
    LLMに “この発言は他ユーザーへの感謝・称賛などのポジティブなフィードバックを含んでいるか？ 含んでいるなら対象ユーザーIDを返して” と聞く。
    """
    # まず旧来のキーワード式でざっくりフィルタ
    if not any(kw in text for kw in POSITIVE_KEYWORDS):
        return []

    # OpenAI APIキーがない場合やエラー時は、従来ロジックで
    if not openai.api_key:
        return re.findall(r"<@([A-Z0-9]+)>", text)

    # LLM プロンプトを組み立て
    try:
        system = "あなたはSlackコミュニティ運営ボットです。"
        user_prompt = (
            "以下の投稿が他ユーザーへの「感謝」や「称賛」などの、ポジティブなフィードバックを含んでいるか？\n"
            f"投稿:```{text}```\n"
            "含んでいる場合はメンションされたユーザーIDをJSONリストで、含んでいない場合は空リスト([])で答えてください。"
        )
        create_args = {
            "model": MODEL,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user_prompt},
            ],
        }
        # o4-mini系はtemperature指定しない
        if not MODEL.startswith("o4-"):
            create_args["temperature"] = 0

        logger.info(f"[LLM: {MODEL}] detect_positive_feedback req: '{text[:80]}'")
        resp = openai.chat.completions.create(**create_args)
        content = resp.choices[0].message.content.strip()
        logger.info(f"[LLM: {MODEL}] detect_positive_feedback resp: '{content[:80]}'")

        # LLMが["Uxxxx"]の形で返す場合
        ids = json.loads(content)
        if isinstance(ids, list):
            return ids
    except Exception as e:
        #print(f"LLM failed: {e}")  # 必要に応じてログ
        pass

    # 失敗したら従来ロジック   
    return re.findall(r"<@([A-Z0-9]+)>", text)

'''
def is_likely_answer(text: str) -> bool:
    """
    質問への回答とみなせるかのルール判定
    - ネガティブキーワード排除
    - 最低文字数
    """
    # ネガティブ表現があるなら回答除外
    for kw in NEGATIVE_ANSWER_KEYWORDS:
        if kw in text:
            return False
    # 最低文字数 (例: 20字)
    return len(text) >= 20
'''

def is_likely_answer(question: str, answer: str) -> bool:
    """
    質問への回答とみなせるかのルール判定
    LLM に {question, answer} を渡して「Yes/No」で回答判定。
    APIキーがない場合のフォールバックとして、20文字以上なら回答とみなす。
    """
    if openai.api_key:
        messages = [
            {"role": "system", "content": "あなたはSlackのQAコミュニティ運営ボットです。"},
            {"role": "user", "content":
                f"以下は質問です：\n```{question}```\n"
                f"以下はその返信です：\n```{answer}```\n"
                "この返信は質問に対する適切な回答か？Yes/No で答えてください。"
            },
        ]
        create_args = {"model": MODEL, "messages": messages}
        if not MODEL.startswith("o4-"):
            create_args["temperature"] = 0
        
        logger.info(f"[LLM: {MODEL}] is_likely_answer req: question='{question[:80]}', answer='{answer[:80]}'")
        resp = openai.chat.completions.create(**create_args)
        ans = resp.choices[0].message.content.strip().lower()
        logger.info(f"[LLM: {MODEL}] is_likely_answer resp: '{ans[:80]}'")

        return ans.startswith("yes")
    # フォールバック
    return len(answer) >= 20
