from pydantic.dataclasses import dataclass


@dataclass
class SlackMessage:
    text: str
    blocks: list[dict]
