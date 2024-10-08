LOCAL_PATH_TO_RAW_DATA = "data/01_raw"
LOCAL_PATH_TO_INT_DATA = "data/02_intermediate"

PATH_TO_PRICING_IN_LLM_PRICING = "src/lib/data.ts"

SCALE_LEADERBOARD_URL = "https://scale.com/leaderboard"
SCALE_LEADERBOARD_FILE_PREFIX = "scale_leaderboard"
SCALE_EVAL_ADV_ROB = "Adversarial Robustness"
SCALE_COL_MODEL = "Model"
SCALE_EVAL_MAPPING = {
    "Coding": "Coding",
    "Spanish": "Spanish",
    "Instruction Following": "Instruct",
    "Math": "Math",
    "Adversarial Robustness": "Adversarial",
}

HELM_REPO_MAIN = "https://api.github.com/repos/stanford-crfm/helm/commits/main"
HELM_MODEL_URL = "https://raw.githubusercontent.com/stanford-crfm/helm/main/src/helm/config/model_metadata.yaml"
HELM_MODEL_FILE_PREFIX = "helm_models"

LLMPRICING_URL = (
    "https://huggingface.co/spaces/philschmid/llm-pricing/raw/main/src/lib/data.ts"
)
LLMPRICING_API = "https://huggingface.co/api/spaces/philschmid/llm-pricing/"
LLMPRICING_FILE_PREFIX = "llm_pricing"
