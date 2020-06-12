from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from utils.helpers.tg_bot import BotMessageActions


class TelegramButtonSenderOperator(BaseOperator):
    @apply_defaults
    def __init__(self, home_path: str, bot_token: str, chat_id: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.home_path = home_path
        self.bot_token = bot_token
        self.body = {
            "chat_id": chat_id,
            "text": "Ну что, поехали?",
            "reply_markup": {
                "inline_keyboard": [[{"text": "Поехали", "callback_data": "Smirn08_airflow"}]]
            },
        }
        self.bot_actions = BotMessageActions(self.home_path, self.bot_token, self.body)

    def execute(self, *args, **kwargs):
        response = self.bot_actions.send_message
        print(response.text)
