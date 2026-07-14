"""Contract workflow with a slow scripted LLM — lets the recovery demo test
kill -9 a worker mid-extraction without a real API key."""

import asyncio

from ledgerloop.domain.contracts.workflow import ContractIngestionWorkflow
from ledgerloop.runtime.llm import LLM

from .test_contracts import MSA_TERMS
from .test_runtime import FakeResponse


class SlowScriptedClient:
    def __init__(self, delay_s: float = 3.0):
        self.delay_s = delay_s
        self.messages = self

    async def create(self, **request):
        await asyncio.sleep(self.delay_s)  # window for the kill to land mid-call
        return FakeResponse(
            [{"type": "tool_use", "id": "tu_1", "name": "record_terms",
              "input": MSA_TERMS}]
        )


def make() -> ContractIngestionWorkflow:
    return ContractIngestionWorkflow(llm=LLM(client=SlowScriptedClient(), model="fake"))


WORKFLOWS = [make()]
