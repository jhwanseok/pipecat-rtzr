"""Minimal Korean voice bot: RTZR STT -> OpenAI LLM -> Cartesia TTS (SmallWebRTC runner).

NOT YET TESTED against the live RTZR service. Needs RTZR_CLIENT_ID / RTZR_CLIENT_SECRET plus
OPENAI_API_KEY and CARTESIA_API_KEY (and pipecat-ai[runner,webrtc,silero,openai,cartesia]).

    python example.py
"""

import os

from dotenv import load_dotenv
from pipecat.audio.vad.silero import SileroVADAnalyzer
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.runner import PipelineRunner
from pipecat.pipeline.task import PipelineParams, PipelineTask
from pipecat.processors.aggregators.llm_context import LLMContext
from pipecat.processors.aggregators.llm_response_universal import LLMContextAggregatorPair
from pipecat.runner.types import RunnerArguments
from pipecat.runner.utils import create_transport
from pipecat.services.cartesia.tts import CartesiaTTSService
from pipecat.services.openai.llm import OpenAILLMService
from pipecat.transports.base_transport import TransportParams

from pipecat_rtzr import RTZRSTTService

load_dotenv(override=True)

transport_params = {
    "webrtc": lambda: TransportParams(
        audio_in_enabled=True,
        audio_out_enabled=True,
        vad_analyzer=SileroVADAnalyzer(),
    ),
}


async def run_bot(transport):
    stt = RTZRSTTService(settings=RTZRSTTService.Settings(use_punctuation=True))
    llm = OpenAILLMService(api_key=os.environ["OPENAI_API_KEY"])
    tts = CartesiaTTSService(
        api_key=os.environ["CARTESIA_API_KEY"], voice_id="YOUR_KOREAN_VOICE_ID"
    )

    context = LLMContext([{"role": "system", "content": "간결하게 한국어로 답하세요."}])
    aggregators = LLMContextAggregatorPair(context)

    pipeline = Pipeline(
        [
            transport.input(),
            stt,
            aggregators.user(),
            llm,
            tts,
            transport.output(),
            aggregators.assistant(),
        ]
    )
    task = PipelineTask(pipeline, params=PipelineParams(enable_metrics=True))
    await PipelineRunner().run(task)


async def bot(runner_args: RunnerArguments):
    transport = await create_transport(runner_args, transport_params)
    await run_bot(transport)


if __name__ == "__main__":
    from pipecat.runner.run import main

    main()
