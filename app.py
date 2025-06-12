import os
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
from dotenv import load_dotenv

# .env から読み込む
load_dotenv()
app = App(token=os.environ["SLACK_BOT_TOKEN"])

# メンションされたら応答
@app.event("app_mention")
def handle_mention(event, say):
    user = event["user"]
    say(f"<@{user}> さん、こんにちは！ これから監視Bot開発を始めましょう🚀")

if __name__ == "__main__":
    handler = SocketModeHandler(app, os.environ["SLACK_APP_TOKEN"])
    handler.start()
