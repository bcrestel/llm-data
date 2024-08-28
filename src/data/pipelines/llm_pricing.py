from src.utils.constant import LLMPRICING_URL

class LLMPricing:
    def __init__(self, url: str = LLMPRICING_URL) -> None:
        self.url = url
    
    def get_raw_data(self) -> None:
        # TODO: get raw data for LLM Pricing
        raise NotImplementedError

