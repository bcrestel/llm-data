LOCAL_PATH_TO_RAW_DATA = "data/01_raw"
PATH_TO_RAW_DATA_LOG = "data/01_raw/raw_data_log.json"
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

HELM_LATEST_VERSION = "1.7.0"
HELM_MODEL_URL = f"https://crfm.stanford.edu/helm/lite/v{HELM_LATEST_VERSION}/#/models"
HELM_LEADERBOARD_FILE_PREFIX = "helm_models"
